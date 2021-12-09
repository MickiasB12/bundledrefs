// Jacob Nelson
//
// This file implements a bundle as a linked list of bundle entries. A bundle is
// prepared by CASing the head of the bundle to a pending entry.

#ifndef BUNDLE_GRAPH_BUNDLE
#define BUNDLE_GRAPH_BUNDLE


#include <pthread.h>
#include <sys/types.h>
#include <list>
#include <atomic>
#include <mutex>
#include <vector>
#include <algorithm>
#include "graph_common_bundle.h"
#include "plaf.h"
#include "rq_debugging.h"

#define CPU_RELAX asm volatile("pause\n" ::: "memory")

#define DEBUG_PRINT(str)                         \
  if ((i + 1) % 10000 == 0) {                    \
    std::cout << str << std::endl << std::flush; \
  }                                              \
  ++i;

enum op { NOP, INSERT, REMOVE };


template <typename NodeType>

class BundleEntry : public std::vector<<BundleEntryBase<NodeType> *> {
 public:
  BundleEntryBase<NodeType> * get(std::size_t i){
    
    auto ret = std::vector<<BundleEntryBase<NodeType> *>::operator[]{i};
    return ret;
  }
};

template <typename NodeType>
class Bundle : public BundleInterface<NodeType> {
 private:
  std::atomic<BundleEntry<NodeType>> head_;
  BundleEntry<NodeType> volatile tail_;
#ifdef BUNDLE_DEBUG
  volatile int updates = 0;
  BundleEntry<NodeType> *volatile last_recycled = nullptr;
  volatile int oldest_edge = 0;
#endif

 public:
  ~Bundle() {
    std::vector<BundleEntryBase<NodeType> *>::iterator i;
    for(i = head_.begin(); i != head_.end(); i++){
      delete *i;
    }
    head_.clear();

  }

  void init() override {
    BundleEntryBase<NodeType> *nullEntry = new BundleEntryBase<NodeType>(BUNDLE_NULL_TIMESTAMP, nullptr);
    head_.emplace_back(nullEntry);
  }

  // Inserts a new rq_bundle_node at the head of the bundle.
  inline void prepare(NodeType *const ptr) override {
    BundleEntry<NodeType> new_head;
    BundleEntryBase<NodeType> *newEntry = new BundleEntryBase<NodeType>(BUNDLE_PENDING_TIMESTAMP, ptr);
    new_head.emplace_back(newEntry);
    
    auto expected;
    while (true) {
      expected = head_;
      // expected = head_;
      new_head.emplace_back(expected.get(0));
      new_head[0]->neighbors.emplace_back(new_head.get(1));
      long i = 0;
      while (expected[0]->ts_ == BUNDLE_PENDING_TIMESTAMP) {
        // DEBUG_PRINT("insertAtHead");
        CPU_RELAX;
      }
      if (head_.compare_exchange_weak(expected, new_head)) {
#ifdef BUNDLE_DEBUG
        ++updates;
#endif
        return;
      }
    }
  }

  // Labels the pending entry to make it visible to range queries.
  inline void finalize(timestamp_t ts) override {
    BundleEntryBase<NodeType> *entry = head_.get(0);
    assert(entry->ts_ == BUNDLE_PENDING_TIMESTAMP);
    entry->ts_ = ts;
  }

  // Returns a vector of references to the nodes that immediately followed at timestamp ts.
  std::vector<NodeType *> getPtrByTimestamp(timestamp_t ts) override {
    // Start at head and work backwards until edge is found.
    auto size = head_.size();
    auto curr = head_.get(0);
    long i = 0;
    while (curr->ts_ == BUNDLE_PENDING_TIMESTAMP) {
      // DEBUG_PRINT("getPtrByTimestamp");
      CPU_RELAX;
    }
    std::vector<NodeType *> returnList;
   
    curr->visited_ = true;
    std::list<<BundleEntryBase<NodeType> *> queue;
    queue.push_back(curr);
    // 'i' will be used to get all adjacent
    // vertices of a vertex
    auto visitedNodes;

    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr->ts_ <= ts){
            returnList.push_back(curr->ptr_);
        }
      for(visitedNodes = curr->neighbors.begin(); 
          visitedNodes != curr->neighbors.end(); ++visitedNodes){
            if(!visitedNodes->visited_){
              visitedNodes->visited_ = true;
              queue.push_back(visitedNodes);
            }
        }
    }
    
    for(auto& u : head_){
      if(u){
        u->visited_ = false;
      }
    }
#ifdef BUNDLE_DEBUG
    if (curr->marked()) {
      std::cout << dump(0) << std::flush;
      exit(1);
    }
#endif
    return returnList;
  }

  // Reclaims any edges that are older than ts. At the moment this should be
  // ordered before adding a new entry to the bundle.
  inline void reclaimEntries(timestamp_t ts) override {
    // Obtain a reference to the pred non-reclaimable entry and first
    // reclaimable one.
    auto pred = head_.get(0);
    long i = 0;
    std::list<<BundleEntryBase<NodeType> *> Q;
    Q.push_back(pred);
    while (pred->ts_ == BUNDLE_PENDING_TIMESTAMP && !Q.empty()) {
      pred = Q.front();
      Q.pop_front();
      
      auto x;
      for(x = pred->neighbors.begin(); x != pred->neighbors.end(); ++x){
        if(!x->visited_){
          x->visited_ = true;
          Q.push_back(x);
        }
      }
    }
    Q.clear();
    SOFTWARE_BARRIER;
    auto currNodes = pred->neighbors;
    
    if (pred->ts = BUNDLE_NULL_TIMESTAMP|| currNodes.empty()) {
      return;  // Nothing to do.
    }

    auto curr = currNodes[0];
    // If there are no active RQs then we can recycle all edges, but the
    // newest (i.e., head). Similarly if the oldest active RQ is newer than
    // the newest entry, we can reclaim all older entries.
    if (ts == BUNDLE_NULL_TIMESTAMP || pred->ts_ <= ts) {
      pred->neighbors = {};
    } else {
      // Traverse from head and remove nodes that are lower than ts.
      Q.push_back(curr);
      while(curr->ptr_ != nullptr && curr->ts_ > ts){
        curr = Q.front();
        Q.pop_front();

        auto x;
        for(x = curr->neighbors.begin(); x != curr->neighbors.end(); ++x){
            if(!x->visited_){
              x->visited_ = true;
              Q.push_back(x);
            }
        }
      }
      Q.clear();

      if(curr->ptr_ != nullptr){
        pred = curr;
        currNodes = curr->neighbors;
        pred->neighbors = {};
      }

    }
#ifdef BUNDLE_DEBUG
    last_recycled = curr;
    oldest_edge = pred->ts_;
#endif

    // Reclaim nodes.
    Q.push_back(curr);
    while(!Q.empty()){
      curr = Q.front();
      Q.pop_front();
      if(curr->ptr_ == nullptr){
        continue;
      } 
      auto x;
      for(x = curr->neighbors.begin(); x != curr->neighbors.end(); ++x){
        if(!x->visited_){
          x->visited_ = true;
          Q.push_back(x);
        }
      }
      pred = curr;
      pred->mark(ts);

#ifndef BUNDLE_CLEANUP_NO_FREE
      delete pred;
#endif
    }

    //return visited back to false
    for(auto& u : head_){
      if(u){
        u->visited_ = false;
      }
    }
  }

  // [UNSAFE] Returns the number of bundle entries.
  int size() override {
    int size = head_.size() - 1;
    
#ifdef BUNDLE_DEBUG
    for(auto& u : head_){
        if(u->marked() || u == nullptr){
          std::cout << dump(0) << std::flush;
          exit(1);
        }
    }
#endif
    return size;
  }

  inline NodeType *first(timestamp_t &ts) override {
    auto entry = head_.get(0);
    ts = entry->ts_;
    return entry->ptr_;
  }

  std::pair<NodeType *, timestamp_t> *get(int &length) override {
    // Find the number of entries in the list.
    auto curr_entry = head_.get(0);
    int size = head_.size() - 1;

    // Build the return array.
    std::pair<NodeType *, timestamp_t> *retarr =
        new std::pair<NodeType *, timestamp_t>[size];
    int pos = 0;
    NodeType *ptr;
    timestamp_t ts;
    std::list<<BundleEntryBase<NodeType> *> queue;
    queue.push_back(curr_entry);

    //Traverse through each node
    while(!queue.empty()){
      curr_entry = queue.front();
      queue.pop_front();
      ptr = curr_entry->ptr_;
      ts = curr_entry->ts_;
      retarr[pos++] = std::pair<NodeType *, timestamp_t>(ptr, ts);

      auto x;
      for(x = curr_entry->neighbors.begin(); x != curr_entry->neighbors.end(); ++x){
        if(!x->visited_){
          x->visited_ = true;
          queue.push_back(x);
        }
      }
    }

    //reverse back visited to false again
    for(auto& u : head_){
      if(u){
        u->visited_ = false;
      }
    }
    length = size;
    return retarr;
  }

  string __attribute__((noinline)) dump(timestamp_t ts) {
    auto curr = head_.get(0);
    std::stringstream ss;

    ss << "(ts=" << ts << ") : ";
    long i = 0;

    std::list<<BundleEntryBase<NodeType> *> queue;
    queue.push_back(curr);

    //Traverse through each node
    while(!queue.empty()){
      curr = queue.front();
      queue.pop_front();
      ss << "<" << curr->ts_ << "," << curr->ptr_ << ">"
         << "-->";
      auto x;
      for(x = curr->neighbors.begin(); x != curr->neighbors.end(); ++x){
        if(!x->visited_){
          x->visited_ = true;
          queue.push_back(x);
        }
      }
    }

    if (curr->ptr == nullptr) {
      ss << "(tail)<" << curr->ts_ << "," << curr->ptr_ << ">";
    } else {
      ss << "(unexpected end)";
    }
#ifdef BUNDLE_DEBUG
    ss << " [updates=" << updates << ", last_recycled=" << last_recycled
       << ", oldest_edge=" << oldest_edge << "]" << std::endl;
#else
    ss << std::endl;
#endif
    return ss.str();
  }

  
};

#endif  // BUNDLE_LINKED_BUNDLE_H