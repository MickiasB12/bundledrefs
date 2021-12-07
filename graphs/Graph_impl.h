#ifndef LAZYLIST_IMPL_H
#define LAZYLIST_IMPL_H

#include <cassert>
#include <csignal>
#include <algorithm>
#include <vector>
#include "Graph.h"
#include "locks_impl.h"

#ifndef casword_t
#define casword_t uintptr_t
#endif

template <typename K, typename V>

class node_t{
    public:
        K key;
        volatile V val;
        std::vector<node_t*> neighbors;
        volatile int lock;
        volatile long long marked;
        Bundle<node_t<K,V>> *rqbundle;
        bool visited;

        ~node_t() {delete rqbundle;}

        template <typename RQProvider>
        bool isMarked(const int tid, RQProvider* const prov) {
            return marked;
        }
};

template <typename K, typename V, class RecManager>
Graph<K, V, RecManager>::Graph(const int numProcesses, const K _KEY_MIN, 
                              const K _KEY_MAX, const V _NO_VALUE)

: recordmgr(new RecManager(numProcesses, SIGQUIT)),
  rqProvider(
          new RQProvider<K, V, node_t<K, V>, bundle_lazylist<K, V, RecManager>,
                         RecManager, true, false>(numProcesses, this,
                                                  recordmgr)),
#ifdef USE_DEBUGCOUNTERS
      counters(new debugCounters(numProcesses))
#endif
    KEY_MIN(_KEY_MIN),
    KEY_MAX(_KEY_MAX),
    NO_VALUE(_NO_VALUE){
    const int tid = 0;
  initThread(tid);

  nodeptr max = new_node(tid, KEY_MAX, 0, NULL);
  head = new_node(tid, KEY_MIN, 0, NULL);

  // Perform linearization of max to ensure bundles correctly added.
  Bundle<node_t<K, V>>* bundles[] = {head->rqbundle, nullptr};
  nodeptr ptrs[] = {max, nullptr};
  rqProvider->prepare_bundles(bundles, ptrs);
  timestamp_t lin_time =
      rqProvider->linearize_update_at_write(tid, &head->neighbors[0], max);
  rqProvider->finalize_bundles(bundles, lin_time);
}

template <typename K, typename V, class RecManager>
Graph<K, V, RecManager>::~Graph(){
    const int dummyTid = 0;
    nodeptr curr;
    std::list<nodeptr> queue;
    queue.push_back(head);
    
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!x->visited){
                        x->visited = true;
                        queue.push_back(x);
                    }
                }
        recordmgr->deallocate(dummyTid, curr);
        totalNodes.clear();
    }

    delete rqProvider;
    recordmgr->printStatus();
    delete recordmgr;
}

template <typename K, typename V, class RecManager>
void Graph<K, V, RecManager>::initThread(const int tid){
    if (init[tid])
        return;
    else
        init[tid] = !init[tid];

  recordmgr->initThread(tid);
  rqProvider->initThread(tid);
}

template <typename K, typename V, class RecManager>
void Graph<K, V, RecManager>::deinitThread(const int tid){
    if (!init[tid])
        return;
    else
        init[tid] = !init[tid];

  recordmgr->deinitThread(tid);
  rqProvider->deinitThread(tid);
}

template<typename K, typename V, class RecManager>
nodeptr Graph<K, V, RecManager>::new_node(const int tid, const K &key,
                                          const V& val,
                                          nodeptr adjNode){
nodeptr nnode = recordmgr->template allocate<node_t<K, V>>(tid);
  if (nnode == NULL) {
    cout << "out of memory" << endl;
    exit(1);
  }
nnode->key = key;
nnode->val = val;
nnode->neighbors.emplace_back(adjNode);
nnode->marked = 0LL;
nnode->lock = false;
nnode->visited = false;
nnode->rqbundle = new Bundle<node_t<K, V>>();
nnode->rqbundle->init();
vectorLock.lock();
totalNodes.emplace_back(nnode);
vectorLock.unlock();
}

template<typename K, typename V, class RecManager>
bool Graph<K, V, RecManager>::contains(const int tid, const K& key){
    recordmgr->leaveQuiescentState(tid, true);
    nodeptr curr;
    std::list<nodeptr> queue;
    queue.push_back(head);
    V res = NO_VALUE;
    bool check = false;
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if((curr->key == key) && !curr->marked){
            res = curr->val;
            goto endOfTheLoop;
        }
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!x->visited){
                        x->visited = true;
                        queue.push_back(x);
                    }
                    if((x->key == key) && !x->marked){
                        res = x->val;
                        goto endOfTheLoop;
                    }
                }
    }
    endOfTheLoop:
        if(res!= NO_VALUE){
            check = true
        }
    for(auto& u : totalNodes){
        if(u){
            u->visited = false;
        }
    }
    recordmgr->enterQuiescentState(tid);
    return check;
    


}

template <typename K, typename V, class RecManager>
V Graph<K, V, RecManager>::doInsert(const int tid, const K& key,
                                    const V& val, bool onlyIfAbsent){
    nodeptr pred;
    nodeptr curr;
    nodeptr newnode;
    V result;
    while(true){
        recordmgr->leaveQuiescentState(tid);
        curr = totalNodes[rand() % totalNodes.size()];
        pred = totalNodes[rand() % totalNodes.size()];
        if(curr == pred){
            curr = pred->neighbors[0];
        }
        acquireLock(&(pred->lock));
        acquireLock(&(curr->lock));
        if(curr->key == key){
            if(curr->marked){
                releaseLock(&(curr->lock));
                releaseLock(&(pred->lock));
                recordmgr->enterQuiescentState(tid);
                continue;
            } 
            if (onlyIfAbsent) {
                V result = curr->val;
                releaseLock(&(curr->lock));
                releaseLock(&(pred->lock));
                
                recordmgr->enterQuiescentState(tid);
                return result;
            }
            cout << "ERROR: insert-replace functionality not implemented for "
                "graph_bundled at this time."
             << endl;
            exit(-1);
        }
        assert(curr->key != key);
        result = NO_VALUE;
        newnode = new_node(tid, key, val, nullptr);
        // curr->neighbors.emplace_back(newnode);

        Bundle<node_t<K,V>> *bundles[] = {newnode->rqbundle, pred->rqbundle, nullptr};
        nodeptr ptrs[] = {curr, newnode, nullptr};
        rqProvider->prepare_bundles(bundles, ptrs);

        // Perform original linearization.
        timestamp_t lin_time =
          rqProvider->linearize_update_at_write_for_graphs(tid, &pred, newnode, &newnode, curr);
        
        // Finalize bundles.
        rqProvider->finalize_bundles(bundles, lin_time);
        releaseLock(&(curr->lock));
        releaseLock(&(pred->lock));
        recordmgr->enterQuiescentState(tid);
        return result;
    }
}
void Graph<K, V, RecManager>::eraseNeighbors(nodeptr node, const K& key, timestamp_t& lin_time){
    for(auto& x : xnode->neighbors){
        if(x->key == key){
            acquireLock(&(x->lock));
            Bundle<node_t<K, V>>* bundles[] = {x->rqbundle, nullptr};
            nodeptr ptrs[] = {nullptr, nullptr};
            rqProvider->prepare_bundles(bundles, ptrs);
            rqProvider->finalize_bundles(bundles, lin_time);
            node->neighbors.erase(std::remove(node->neighbors.begin(), node->neighbors.end(),x), node->neighbors.end());
            releaseLock(&(x->lock));
        }
    }
}
V Graph<K, V, RecManager>::erase(const int tid, const K& key){
    nodeptr pred;
    nodeptr curr;
    V result = NO_VALUE;
    std::list<nodeptr> queue;
    timestamp_t lin_time = get_update_lin_time(tid);;
    while(true){
        recordmgr->leaveQuiescentState(tid);
        for(auto& u : totalNodes){
            if(u->key == key){
                result = u->value;
                acquireLock(&(u->lock));
                lin_time = rqProvider->linearize_update_at_write(tid, &u->marked, 1LL);
                u->neighbors.clear();
                nodeptr deletedNodes[] = {u, nullptr};
                rqProvider->physical_deletion_succeeded(tid, deletedNodes);
                vectorLock.lock();
                totalNodes.erase(std::remove(totalNodes.begin(), totalNodes.end(), u), totalNodes.end());
                vectorLock.unlock();
                releaseLock(&(u->lock));
            }
            else{
                eraseNeighbors(u, key, lin_time)
            }
        }
        // queue.push_back(head);

        // while(!queue.empty()){
        //     curr = queue.front();
        //     queue.pop_front();
        //     if((curr->key == key)){
        //         result = curr->val;
        //         goto endOfTheLoop;
        //     }
        //         for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
        //             if(!x->visited){
        //                 x->visited = true;
        //                 queue.push_back(x);
        //             }
        //             if((x->key == key)){
        //                 curr = x;
        //                 goto endOfTheLoop;
        //             }
        //         }
        // }
        // if(curr->key != key){
        //     result = NO_VALUE;
        //     recordmgr->enterQuiescentState(tid);
        //     return result;
        // }
        // endOfTheLoop:
        //     acquireLock(&(curr->lock));
        //     assert(curr->key == key);
        //     result = curr->val;

        //     Bundle<node_t<K, V>>* bundles[] = {}


        recordmgr->enterQuiescentState(tid);
        return result;
    }

}

template<typename K, typename V, class RecManager>
int Graph<K, V, RecManager>::rangeQuery(const int tid, const K& lo,
                                        const K& hi, 
                                        K* const resultKeys,
                                        V* const resultValues){

    timestamp_t ts;
    int cnt = 0;
    std::vector<nodeptr> adjList;
    
        recordmgr->leaveQuiescentState(tid, true);

        // Read gloabl timestamp and announce self.
        ts = rqProvider->start_traversal(tid);
        adjList = head->rqbundle->getPtrByTimestamp(ts);
        nodeptr curr;
        while(!adjList.empty){
            for(curr : adjList){
                if((curr) && (curr->key <= hi || curr->key >= lo)){
                    cnt += getKeys(tid, curr, resultKeys + cnt, resultValues + cnt);
                }
            }
            adjList = curr->rqbundle->getPtrByTimestamp(ts);
        }
        
        // Clears entry in active range query array.
        rqProvider->end_traversal(tid);
        recordmgr->enterQuiescentState(tid);

        // Traversal was completed successfully.
        return cnt;
}

template <typename K, typename V, class RecManager>
void Graph<K, V, RecManager>::cleanup(int tid){
    // Walk the list using the newest edge and reclaim bundle entries.
    std::list<nodeptr> queue;
    nodeptr curr;
    recordmgr->leaveQuiescentState(tid); 
    BUNDLE_INIT_CLEANUP(rqProvider);
    while (head == nullptr);
    queue.push_back(head);
   while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr->key != KEY_MAX){
            BUNDLE_CLEAN_BUNDLE(curr->rqbundle);
        }
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!x->visited){
                        x->visited = true;
                        queue.push_back(x);
                    }
                }
    }
    for(auto& u : totalNodes){
      if(u){
        u->visited = false;
      }
    }

    recordmgr->enterQuiescentState(tid);
}

template <typename K, typename V, class RecManager>
bool Graph<K, V, RecManager>::validateBundles(int tid){
    nodeptr curr;
    nodeptr temp;
    timestamp_t ts;
    std::list<nodeptr> queue;
    queue.push_back(head);
    bool valid = true;
#ifdef BUNDLE_DEBUG
     while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr->key < KEY_MAX){
            result += curr->key;
        }
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!x->visited){
                        x->visited = true;
                        queue.push_back(x);
                    }
                }
    }
    for(auto& u : totalNodes){
      if(u){
        u->visited = false;
      }
    }
#endif
    return valid;
}

template <typename K, typename V, class RecManager>
long long Graph<K, V, RecManager>::debugKeySum(nodeptr head){
    long long result = 0;
    std::list<nodeptr> queue;
    queue.push_back(head);
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr->key < KEY_MAX){
            result += curr->key;
        }
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!x->visited){
                        x->visited = true;
                        queue.push_back(x);
                    }
                }
    }
    for(auto& u : totalNodes){
      if(u){
        u->visited = false;
      }
    }
    return result;
}

template <typename K, typename V, class RecManager>
long long Graph<K, V, RecManager>::debugKeySum(){
    return debugKeySum(head);
}


template <typename K, typename V, class RecManager>
inline bool Graph<K, V, RecManager>::isLogicallyDeleted(const int tid, node_t<K, V>* node){
    return node->isMarked(tid, rqProvider);
}



