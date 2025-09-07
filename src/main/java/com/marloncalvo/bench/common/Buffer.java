package com.marloncalvo.bench.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Buffer<T> implements Collection<T> {
    private final T[] elements;
    private int offset = 0;

    @SuppressWarnings("unchecked")
	public Buffer(int size) {
        this.elements = (T[]) new Object[size];
    }

	@Override
	public int size() {
		return offset;
	}

	@Override
	public boolean isEmpty() {
		return offset == 0;
	}

	public boolean isFull() {
		return offset == elements.length;
	}

	@Override
	public boolean contains(Object o) {
		for (int i = 0; i < offset; i++) {
			if (elements[i].equals(o)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			private int current = 0;

			@Override
			public boolean hasNext() {
				return current < offset;
			}

			@Override
			public T next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return elements[current++];
			}
		};
	}

	@Override
	public Object[] toArray() {
		return Arrays.copyOf(elements, offset);
	}

	@Override
	public <K> K[] toArray(K[] a) {
		return (K[]) Arrays.copyOf(elements, offset, a.getClass());
	}

	@Override
	public boolean add(T e) {
		if (offset == elements.length) {
			throw new IllegalStateException("Buffer is full");
		}
		elements[offset++] = e;
		return true;
	}

	@Override
	public boolean remove(Object o) {
		return true;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (var e : c) {
            if (!this.contains(e)) {
                return false;
            }
        }
        return true;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		for (var e : c) {
            this.add(e);
        }
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		for (var e : c) {
            this.remove(e);
        }
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		for (int i = 0; i < offset; i++) {
			if (!c.contains(elements[i])) {
				remove(elements[i]);
				i--;
			}
		}
		return true;
	}

	@Override
	public void clear() {
		offset = 0;
	}
}
