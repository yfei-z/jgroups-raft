package org.jgroups.raft.blocks;

import static org.jgroups.raft.blocks.LockService.LockStatus.HOLDING;
import static org.jgroups.raft.blocks.LockService.LockStatus.NONE;
import static org.jgroups.raft.blocks.LockService.LockStatus.WAITING;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.jgroups.Address;
import org.jgroups.ChannelListener;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;

/**
 * @author Zhang Yifei
 */
@Experimental
public class LockService {
	protected static final Log log = LogFactory.getLog(LockService.class);

	protected static final byte LOCK = 1, TRY_LOCK = 2, UNLOCK = 3, UNLOCK_ALL = 4, RESET = 5;

	protected final RaftHandle raft;
	protected final Map<Long, LockEntry> entries = new HashMap<>();
	protected final Map<UUID, Set<LockEntry>> memberToEntries = new LinkedHashMap<>();

	protected volatile View view;
	protected ExtendedUUID address;

	protected final ConcurrentMap<Long, LockStatus> lockStatus = new ConcurrentHashMap<>();
	protected final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
	protected final ConcurrentMap<Long, MutexImpl> mutexes = new ConcurrentHashMap<>(); // could be weak value reference

	public LockService(JChannel channel) {
		if (channel.isConnecting() || channel.isConnected()) {
			throw new IllegalStateException("Illegal channel state " + channel.getState());
		}
		Hook hook = createHook();
		raft = createRaft(channel, hook);
		channel.setUpHandler(hook).addChannelListener(hook);
		raft.addRoleListener(hook);
	}

	protected Hook createHook() {
		return new Hook();
	}

	protected RaftHandle createRaft(JChannel ch, StateMachine sm) {
		return new RaftHandle(ch, sm);
	}

	protected class Hook implements StateMachine, RAFT.RoleChange, UpHandler, ChannelListener {

		@Override
		public void readContentFrom(DataInput in) {
			Map<Long, LockStatus> tmp = new HashMap<>();
			entries.clear(); memberToEntries.clear();
			for (int i = 0, l = readInt(in); i < l; i++) {
				long key = readLong(in);
				LockEntry entry = new LockEntry(key);
				entry.holder = readUuid(in);
				for (int t = 0, m = readInt(in); t < m; t++) {
					entry.waiters.add(readUuid(in));
				}
				entries.put(key, entry);
				bind(entry.holder, entry);
				if (address.equals(entry.holder)) tmp.put(entry.key, HOLDING);
				for (var waiter : entry.waiters) {
					bind(waiter, entry);
					if (address.equals(waiter)) tmp.put(entry.key, WAITING);
				}
			}

			// notify base on local status
			lockStatus.forEach((k, v) -> notifyListeners(k, v, tmp.remove(k), false));
			tmp.forEach((k, v) -> notifyListeners(k, NONE, v, false));
		}

		@Override
		public void writeContentTo(DataOutput out) {
			writeInt((int) entries.values().stream().filter(t -> t.holder != null).count(), out);
			for (var lock : entries.values()) {
				if (lock.holder == null) continue;
				writeLong(lock.key, out);
				writeUuid(lock.holder, out);
				writeInt(lock.waiters.size(), out);
				for (UUID t : lock.waiters) {
					writeUuid(t, out);
				}
			}
		}

		@Override
		public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
			var in = new ByteArrayDataInputStream(data, offset, length);
			LockStatus status = null;
			switch (in.readByte()) {
				case LOCK:
					status = doLock(readLong(in), readUuid(in), false);
					break;
				case TRY_LOCK:
					status = doLock(readLong(in), readUuid(in), true);
					break;
				case UNLOCK:
					LockEntry entry = entries.computeIfAbsent(readLong(in), LockEntry::new);
					doUnlock(readUuid(in), entry, null);
					break;
				case UNLOCK_ALL:
					doUnlock(readUuid(in), null);
					break;
				case RESET:
					int len = readInt(in);
					List<UUID> members = new ArrayList<>(len);
					for (int i = 0; i < len; i++) {
						members.add(readUuid(in));
					}
					doReset(members);
					break;
			}
			return serialize_response && status != null ? new byte[] {(byte) status.ordinal()} : null;
		}

		@Override
		public void roleChanged(Role role) {
			if (role == Role.Leader) {
				try {
					// Reset after the leader is elected
					View v = view;
					boolean mergedMinorities = false;
					if (v instanceof MergeView) {
						int majority = raft.raft().majority();
						mergedMinorities = ((MergeView) v).getSubgroups().stream().allMatch(t -> t.size() < majority);
					}
					reset(mergedMinorities ? null : v);
				} catch (Throwable e) {
					log.error("Fail to send reset command", e);
				}
			}
		}

		@Override
		public UpHandler setLocalAddress(Address a) {
			address = (ExtendedUUID) a; return this;
		}

		@Override
		public Object up(Event evt) {
			if (evt.getType() == Event.VIEW_CHANGE) {
				handleView(evt.arg());
			}
			return null;
		}

		@Override
		public Object up(Message msg) {return null;}

		@Override
		public void channelDisconnected(JChannel channel) {
			cleanup();
		}
	}

	protected static class LockEntry {
		public final long key;
		public final LinkedHashSet<UUID> waiters = new LinkedHashSet<>();
		public UUID holder;

		protected LockEntry(long key) {this.key = key;}

		protected UUID unlock() {
			// make sure it's a consistent result for all nodes
			var i = waiters.iterator();
			if (!i.hasNext()) return holder = null;
			var v = i.next(); i.remove(); return holder = v;
		}
	}

	protected LockStatus doLock(long key, UUID id, boolean trying) {
		LockEntry entry = entries.computeIfAbsent(key, LockEntry::new);
		LockStatus prev = NONE, next = HOLDING;
		if (entry.holder == null) {
			entry.holder = id;
		} else if (entry.holder.equals(id)) {
			prev = HOLDING;
		} else if (trying) {
			prev = next = entry.waiters.contains(id) ? WAITING : NONE;
		} else {
			if (!entry.waiters.add(id)) prev = WAITING;
			next = WAITING;
		}
		if (prev != next) bind(id, entry);
		if (address.equals(id)) {
			notifyListeners(entry.key, prev, next, false);
		}
		if (log.isDebugEnabled()) {
			log.debug("[%s] %s lock %s, prev: %s, next: %s", address, id, entry.key, prev, next);
		}
		return next;
	}

	protected void doUnlock(UUID id, Set<UUID> unlocking) {
		Set<LockEntry> set = memberToEntries.get(id); if (set == null) return;
		for (LockEntry entry : set.toArray(LockEntry[]::new)) {
			doUnlock(id, entry, unlocking);
		}
	}

	protected void doUnlock(UUID id, LockEntry entry, Set<UUID> unlocking) {
		LockStatus prev = HOLDING;
		UUID holder = null;
		List<UUID> waiters = null;
		if (id.equals(entry.holder)) {
			do {
				if (holder != null) {
					if (waiters == null) waiters = new ArrayList<>(unlocking.size());
					waiters.add(holder);
				}
				holder = entry.unlock();
			} while (holder != null && unlocking != null && unlocking.contains(holder));
		} else {
			prev = entry.waiters.remove(id) ? WAITING : NONE;
		}
		if (prev != NONE) unbind(id, entry);
		if (address.equals(id)) {
			notifyListeners(entry.key, prev, NONE, false);
		} else if (address.equals(holder)) {
			notifyListeners(entry.key, WAITING, HOLDING, false);
		}
		if (log.isDebugEnabled()) {
			log.debug("[%s] %s unlock %s, prev: %s", address, id, entry.key, prev);
			if (holder != null)
				log.debug("[%s] %s lock %s, prev: %s, next: %s", address, holder, entry.key, WAITING, HOLDING);
		}
		if (waiters != null) for (UUID waiter : waiters) {
			unbind(waiter, entry);
			if (address.equals(waiter)) {
				notifyListeners(entry.key, WAITING, NONE, false);
			}
			if (log.isDebugEnabled()) {
				log.debug("[%s] %s unlock %s, prev: %s", address, waiter, entry.key, WAITING);
			}
		}
	}

	protected void doReset(List<UUID> members) {
		Set<UUID> prev = new LinkedHashSet<>(memberToEntries.keySet());
		if (log.isDebugEnabled()) {
			log.debug("[%s] reset %s to %s", address, prev, members);
		}
		for (var id : members) prev.remove(id);
		for (var id : prev) doUnlock(id, prev);
	}

	protected void bind(UUID id, LockEntry entry) {
		memberToEntries.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(entry);
	}

	protected void unbind(UUID id, LockEntry entry) {
		memberToEntries.computeIfPresent(id, (k, v) -> {
			v.remove(entry); return v.isEmpty() ? null : v;
		});
	}

	protected void notifyListeners(long key, LockStatus prev, LockStatus curr, boolean force) {
		if (!force && raft.leader() == null) return;
		if (prev == null) prev = NONE;
		if (curr == null) curr = NONE;
		LockStatus local = curr == NONE ? lockStatus.remove(key) : lockStatus.put(key, curr);
		if (prev == curr) {
			prev = local == null ? NONE : local;
			if (prev == curr) return;
		}
		MutexImpl mutex = mutexes.get(key);
		if (mutex != null) mutex.onStatusChange(key, prev, curr);
		for (Listener listener : listeners) {
			try {
				listener.onStatusChange(key, prev, curr);
			} catch (Throwable e) {
				log.error("Fail to notify listener, lock: %s, prev: %s, curr: %s", key, prev, curr, e);
			}
		}
	}

	protected void handleView(View next) {
		View prev = this.view; this.view = next;
		if (log.isDebugEnabled()) {
			log.debug("[%s] View accepted: %s, prev: %s, leader: %s", address, next, prev, raft.leader());
		}

		if (prev != null) {
			int majority = raft.raft().majority();
			if (prev.size() >= majority && next.size() < majority) { // lost majority
				// In partition case if majority is still working, it will be forced to unlock by reset command.
				cleanup();
			} else if (!next.containsMembers(prev.getMembersRaw()) && raft.isLeader()) { // member left
				try {
					reset(next);
				} catch (Throwable e) {
					log.error("Fail to send reset command", e);
				}
			}
		}
	}

	protected void cleanup() {
		lockStatus.forEach((k, v) -> notifyListeners(k, v, NONE, true));
	}

	protected void reset(View view) {
		Address[] members = view != null ? view.getMembersRaw() : new Address[0];
		int len = members.length;
		var out = new ByteArrayDataOutputStream(6 + len * 16);
		out.writeByte(RESET);
		writeInt(len, out);
		for (Address member : members) {
			writeUuid((UUID) member, out);
		}
		assert out.position() <= 6 + len * 16;
		invoke(out).thenApply(t -> null).exceptionally(e -> {
			log.error("Fail to reset to " + view, e); return null;
		});
	}

	/**
	 * Add listener
	 * @param listener listener for the status change.
	 * @return true if added, otherwise false.
	 */
	public boolean addListener(Listener listener) { return listeners.addIfAbsent(listener); }

	/**
	 * Remove listener
	 * @param listener listener for removing
	 * @return true if removed, otherwise false.
	 */
	public boolean removeListener(Listener listener) { return listeners.remove(listener); }

	/**
	 * Get lock status from local.
	 * @param key the key to query
	 * @return the status for the key
	 */
	public LockStatus localStatus(long key) {
		var v = lockStatus.get(key); return v == null ? NONE : v;
	}

	/**
	 * Lock the specified key.
	 * @param key the key to lock
	 * @return HOLDING if locked the key, WAITING if lock is hold by another node, lock request is queued.
	 */
	public CompletableFuture<LockStatus> lock(long key) {
		var out = new ByteArrayDataOutputStream(26);
		out.writeByte(LOCK);
		writeLong(key, out);
		writeUuid(address(), out);
		assert out.position() <= 26;
		return invoke(out).thenApply(t -> LockStatus.values()[t[0]]);
	}

	/**
	 * Try to lock the specified key. The request won't be queued if currently lock is not available.
	 * @param key the key to lock
	 * @return HOLDING if locked the key, NONE if lock is not available.
	 */
	public CompletableFuture<LockStatus> tryLock(long key) {
		var out = new ByteArrayDataOutputStream(26);
		out.writeByte(TRY_LOCK);
		writeLong(key, out);
		writeUuid(address(), out);
		assert out.position() <= 26;
		return invoke(out).thenApply(t -> LockStatus.values()[t[0]]);
	}

	/**
	 * Unlock the key for this node, and pick next acquirer to be the holder if there are queued acquirers.
	 * @param key the key to unlock
	 * @return async completion
	 */
	public CompletableFuture<Void> unlock(long key) {
		var out = new ByteArrayDataOutputStream(26);
		out.writeByte(UNLOCK);
		writeLong(key, out);
		writeUuid(address(), out);
		assert out.position() <= 26;
		return invoke(out).thenApply(t -> null);
	}

	/**
	 * Unlock all for this node
	 * @return async completion
	 */
	public CompletableFuture<Void> unlock() {
		var out = new ByteArrayDataOutputStream(17);
		out.writeByte(UNLOCK_ALL);
		writeUuid(address(), out);
		assert out.position() <= 17;
		return invoke(out).thenApply(t -> null);
	}

	protected UUID address() {
		return Objects.requireNonNull(address);
	}

	protected CompletableFuture<byte[]> invoke(ByteArrayDataOutputStream out) {
		// TODO It is interruptible when current thread calls RAFT directly
//		boolean interrupted = Thread.interrupted();
		try {
			return raft.setAsync(out.buffer(), 0, out.position());
		} catch (Throwable e) {
			throw new RaftException("Fail to execute command", e);
//		} finally {
//			if (interrupted) Thread.currentThread().interrupt();
		}
	}

	/**
	 * Get the mutex
	 * @param key the key related to the mutex
	 * @return mutex instance
	 */
	public Mutex mutex(long key) {
		return mutexes.computeIfAbsent(key, MutexImpl::new);
	}

	/**
	 * The status of the node for a lock
	 */
	public enum LockStatus {
		HOLDING, WAITING, NONE
	}

	/**
	 * Listen on the lock status changes
	 */
	public interface Listener {
		void onStatusChange(long key, LockStatus prev, LockStatus curr);
	}

	/**
	 * Exception for the raft cluster errors
	 */
	public static class RaftException extends RuntimeException {
		public RaftException(String message) { super(message); }
		public RaftException(Throwable cause) { super(cause); }
		public RaftException(String message, Throwable cause) { super(message, cause); }
	}

	/**
	 * Distributed lock
	 */
	public interface Mutex extends Lock {

		/**
		 * @throws RaftException if exception happens during sending or executing commands.
		 */
		void lock();

		/**
		 * @throws RaftException if exception happens during sending or executing commands.
		 */
		void lockInterruptibly() throws InterruptedException;

		/**
		 * @throws RaftException if exception happens during sending or executing commands.
		 */
		boolean tryLock();

		/**
		 * @throws RaftException if exception happens during sending or executing commands.
		 */
		boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

		/**
		 * To be implemented
		 */
		default Condition newCondition() {
			throw new UnsupportedOperationException();
		}

		/**
		 * @throws RaftException if exception happens during sending or executing commands.
		 */
		void unlock();

		/**
		 * The lock status of raft service
		 * @return lock status
		 */
		LockStatus getStatus();

		/**
		 * The current holder of the lock
		 * @return the thread which holding the lock
		 */
		Thread getHolder();

		/**
		 * Register a handler for the unexpected unlocking of the raft service.
		 * @param handler callback with this mutex
		 */
		void setUnexpectedUnlockHandler(Consumer<Mutex> handler);

		/**
		 * Register a handler for the unexpected locking of the raft service.
		 * @param handler callback with this mutex
		 */
		void setUnexpectedLockHandler(Consumer<Mutex> handler);

		/**
		 * Executing command timeout
		 * @param timeout in milliseconds
		 */
		void setTimeout(long timeout);

		/**
		 * Get the LockService which create the mutex from.
		 * @return underlying service
		 */
		LockService service();
	}

	protected class MutexImpl implements Mutex, Listener {
		private final long key;
		private volatile LockStatus status = NONE;
		private volatile Thread holder;
		private final AtomicInteger acquirers = new AtomicInteger();
		private final ReentrantLock delegate = new ReentrantLock();
		private final Condition notWaiting = delegate.newCondition();
		private Consumer<Mutex> lockHandler, unlockHandler;
		private long timeout = 8000;

		public MutexImpl(long key) {this.key = key;}

		@Override
		public void setTimeout(long timeout) {this.timeout = timeout;}

		@Override
		public LockStatus getStatus() {return status;}

		@Override
		public Thread getHolder() {return holder;}

		@Override
		public void setUnexpectedLockHandler(Consumer<Mutex> handler) {lockHandler = handler;}

		@Override
		public void setUnexpectedUnlockHandler(Consumer<Mutex> handler) {unlockHandler = handler;}

		@Override
		public LockService service() {return LockService.this;}

		@Override
		public void lock() {
			delegate.lock();
			acquirers.incrementAndGet();
			while (status != HOLDING) {
				try {
					if (status == WAITING) notWaiting.awaitUninterruptibly();
					else status = join(LockService.this.lock(key));
				} catch (Throwable e) {
					rethrow(unlock(e));
				}
			}
			holder = Thread.currentThread();
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			delegate.lockInterruptibly();
			acquirers.incrementAndGet();
			while (status != HOLDING) {
				try {
					if (status == WAITING) notWaiting.await();
					else status = join(LockService.this.lock(key));
				} catch (InterruptedException e) {
					throw unlock(e);
				} catch (Throwable e) {
					rethrow(unlock(e));
				}
			}
			holder = Thread.currentThread();
		}

		@Override
		public boolean tryLock() {
			if (!delegate.tryLock()) return false;
			acquirers.incrementAndGet();
			if (status == NONE) {
				try {
					status = join(LockService.this.tryLock(key));
				} catch (Throwable ignored) {
				}
			}
			if (status == HOLDING) {
				holder = Thread.currentThread(); return true;
			}
			unlock(); return false;
		}

		@Override
		public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
			long deadline = System.nanoTime() + unit.toNanos(timeout), ns;
			if (!delegate.tryLock(timeout, unit)) return false;
			acquirers.incrementAndGet();
			while (status != HOLDING && (ns = deadline - System.nanoTime()) > 0) {
				try {
					if (status == WAITING) notWaiting.awaitNanos(ns);
					else status = join(LockService.this.lock(key));
				} catch (InterruptedException e) {
					throw unlock(e);
				} catch (Throwable e) {
					rethrow(unlock(e));
				}
			}
			if (status == HOLDING) {
				holder = Thread.currentThread(); return true;
			}
			unlock(); return false;
		}

		@Override
		public void unlock() {
			if (!delegate.isHeldByCurrentThread()) return;
			assert holder == null || holder == Thread.currentThread();
			if (delegate.getHoldCount() == 1) holder = null;
			try {
				if (acquirers.decrementAndGet() == 0 && status != NONE) {
					join(LockService.this.unlock(key));
					status = NONE;
				}
			} catch (Throwable e) {
				rethrow(e);
			} finally {
				delegate.unlock();
			}
		}

		private <T extends Throwable> T unlock(T error) {
			try {
				unlock();
			} catch (Throwable e) {
				error.addSuppressed(e);
			}
			return error;
		}

		private <T> T join(CompletableFuture<T> future) throws ExecutionException, TimeoutException {
			long nanos = TimeUnit.MILLISECONDS.toNanos(timeout), deadline = System.nanoTime() + nanos;
			boolean interrupted = Thread.interrupted();
			try {
				do {
					try {
						return future.get(nanos, TimeUnit.NANOSECONDS);
					} catch (InterruptedException e) {
						interrupted = true;
					}
				} while ((nanos = deadline - System.nanoTime()) > 0);
				throw new TimeoutException();
			} finally {
				if (interrupted) Thread.currentThread().interrupt();
			}
		}

		@Override
		public void onStatusChange(long key, LockStatus prev, LockStatus curr) {
			if (key != this.key) return;
			if (curr != HOLDING && holder != null) {
				status = curr;
				var handler = unlockHandler;
				if (handler != null) try {
					handler.accept(this);
				} catch (Throwable e) {
					log.error("Error occurred on unlock handler", e);
				}
			} else if (curr != NONE && acquirers.get() == 0) {
				status = curr;
				var handler = lockHandler;
				if (handler != null) try {
					handler.accept(this);
				} catch (Throwable e) {
					log.error("Error occurred on lock handler", e);
				}
			} else if (prev == WAITING) {
				delegate.lock();
				try {
					if (status == WAITING) {
						status = curr;
						notWaiting.signalAll();
					}
				} finally {
					delegate.unlock();
				}
			}
		}
	}

	private static <T> T rethrow(Throwable e) {
		if (e instanceof RaftException) throw (RaftException) e;
		if (e instanceof CompletionException) {
			Throwable cause = e.getCause();
			throw cause != null ? new RaftException(e) : (CompletionException) e;
		}
		if (e instanceof ExecutionException) throw new RaftException(e.getCause());
		if (e instanceof TimeoutException) throw new RaftException("Execute command timeout", e);
		throw new RaftException("Unknown exception", e);
	}

	private static void writeInt(int value, DataOutput out) {
		try {
			for (; (value & ~0x7F) != 0; value >>>= 7) {
				out.writeByte(0x80 | (value & 0x7F));
			}
			out.writeByte(value);
		} catch (IOException e) {
			throw new RaftException("Should never happen", e);
		}
	}

	private static int readInt(DataInput in) {
		try {
			int result = 0; byte b;
			for (int i = 0; i < 28; i += 7) {
				result |= ((b = in.readByte()) & 0x7F) << i;
				if ((b & 0x80) == 0) return result;
			}
			return result | (in.readByte() & 0x0F) << 28;
		} catch (IOException e) {
			throw new RaftException("Should never happen", e);
		}
	}

	private static void writeLong(long value, DataOutput out) {
		try {
			for (int i = 0; i < 8 && (value & ~0x7FL) != 0; i++) {
				out.writeByte(0x80 | ((int) value & 0x7F));
				value >>>= 7;
			}
			out.writeByte((int) value);
		} catch (IOException e) {
			throw new RaftException("Should never happen", e);
		}
	}

	private static long readLong(DataInput in) {
		try {
			long result = 0; byte b;
			for (int i = 0; i < 56; i += 7) {
				result |= ((long) (b = in.readByte()) & 0x7F) << i;
				if ((b & 0x80) == 0) return result;
			}
			return result | ((long) in.readByte() & 0xFF) << 56;
		} catch (IOException e) {
			throw new RaftException("Should never happen", e);
		}
	}

	private static void writeUuid(UUID id, DataOutput out) {
		try {
			out.writeLong(id.getMostSignificantBits());
			out.writeLong(id.getLeastSignificantBits());
		} catch (IOException e) {
			throw new RaftException("Should never happen", e);
		}
	}

	private static UUID readUuid(DataInput in) {
		try {
			return new UUID(in.readLong(), in.readLong());
		} catch (IOException e) {
			throw new RaftException("Should never happen", e);
		}
	}
}
