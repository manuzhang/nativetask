/*
 * heap.h
 *
 *  Created on: 17 Jul 2013
 *      Author: xzhong10
 */

#ifndef HEAP_H_
#define HEAP_H_

#include "NativeTask.h"
#include "Buffers.h"


/**
 * heap used by merge
 */
template<typename T, typename Compare>
void adjust_heap(T* first, int rt, int heap_len, Compare & Comp) {
  while (rt * 2 <= heap_len) // not leaf
  {
    int left = (rt << 1); // left child
    int right = (rt << 1) + 1; // right child
    int smallest = rt;
    if (Comp(*(first + left - 1), *(first + smallest - 1))) {
      smallest = left;
    }
    if (right <= heap_len && Comp(*(first + right - 1), *(first + smallest - 1))) {
      smallest = right;
    }
    if (smallest != rt) {
      std::swap(*(first + smallest - 1), *(first + rt - 1));
      rt = smallest;
    }
    else {
      break;
    }
  }
}

template<typename T, typename Compare>
void make_heap(T* begin, T* end, Compare & Comp) {
  int heap_len = end - begin;
  if (heap_len >= 0) {
    for (int i = heap_len / 2; i >= 1; i--) {
      adjust_heap(begin, i, heap_len, Comp);
    }
  }
}

/**
 * just for test
 */
template<typename T, typename Compare>
void check_heap(T* begin, T* end, Compare & Comp) {
  int heap_len = end - begin;
  if (heap_len >= 0) {
    for (int i = heap_len / 2; i >= 1; i--) {
      int left = i << 1;
      int right = left + 1;
      if (Comp(*(begin+left-1), *(begin+i-1))) {
        assert(false);
      }
      if (right<=heap_len) {
        if (Comp(*(begin+right-1), *(begin+i-1))) {
          assert(false);
        }
      }
    }
  }
}

template<typename T, typename Compare>
void push_heap(T* begin, T* end, Compare & Comp) {
  int now = end - begin;
  while (now > 1) {
    int parent = (now >> 1);
    if (Comp(*(begin + now - 1), *(begin + parent - 1))) {
      std::swap(*(begin + now - 1), *(begin + parent - 1));
      now = parent;
    }
    else {
      break;
    }
  }
}

template<typename T, typename Compare>
void pop_heap(T* begin, T* end, Compare & Comp) {
  *begin = *(end - 1);
  // adjust [begin, end - 1) to heap
  adjust_heap(begin, 1, end - begin - 1, Comp);
}


#endif /* HEAP_H_ */
