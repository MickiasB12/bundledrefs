

#ifndef UNBUNDLED_GRAPH_IMPL_H
#define	UNBUNDLED_GRAPH_IMPL_H

#include <cassert>
#include <csignal>
#include <algorithm>
#include <vector>

#include "locks_impl.h"
#include "unbundled_graph.h"

#ifndef casword_t
#define casword_t uintptr_t
#endif

template <typename K, typename V>

class node_t {
    public:
        K key;
        volatile V val;
        std::vector<node_t<K,V> *> neighbors;
        volatile int lock;
        volatile long long marked;
        volatile long long itime;
        volatile long long dtime;
        volatile bool visited;

        template <typename RQProvider>
        bool isMarked(const int tid, RQProvider* const prov) {
            return prov->read_addr(tid, (casword_t *) &marked);
        }
};

template <typename K, typename V, class RecManager>
unbundled_graph<K, V, RecManager>::unbundled_graph(const int numProcesses, const K _KEY_MIN, 
                              const K _KEY_MAX, const V _NO_VALUE)

: recordmgr(new RecManager(numProcesses, SIGQUIT)),
  rqProvider(
          new RQProvider<K, V, node_t<K, V>, unbundled_graph<K, V, RecManager>,
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

}

template <typename K, typename V, class RecManager>
unbundled_graph<K, V, RecManager>::~unbundled_graph(){
    const int dummyTid = 0;
    nodeptr curr;
    std::list<nodeptr> queue;
    queue.push_back(head);
    
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!(*x)->visited){
                        (*x)->visited = true;
                        queue.push_back(*x);
                    }
                }
        recordmgr->deallocate(dummyTid, curr);
        totalNodes.clear();
    }

    delete rqProvider;
    delete recordmgr;
}

template <typename K, typename V, class RecManager>
void unbundled_graph<K, V, RecManager>::initThread(const int tid){
    if (init[tid])
        return;
    else
        init[tid] = !init[tid];

  recordmgr->initThread(tid);
  rqProvider->initThread(tid);
}

template <typename K, typename V, class RecManager>
void unbundled_graph<K, V, RecManager>::deinitThread(const int tid){
    if (!init[tid])
        return;
    else
        init[tid] = !init[tid];

  recordmgr->deinitThread(tid);
  rqProvider->deinitThread(tid);
}

template<typename K, typename V, class RecManager>
nodeptr unbundled_graph<K, V, RecManager>::new_node(const int tid, const K &key,
                                          const V& val,
                                          nodeptr adjNode){
nodeptr nnode = recordmgr->template allocate<node_t<K, V>>(tid);
  if (nnode == NULL) {
    cout << "out of memory" << endl;
    exit(1);
  }
rqProvider->init_node(tid, nnode);
nnode->key = key;
nnode->val = val;
nnode->neighbors.emplace_back(adjNode);
nnode->marked = 0LL;
nnode->lock = false;
nnode->visited = false;
vectorLock.lock();
totalNodes.emplace_back(nnode);
vectorLock.unlock();
return nnode;
}

template<typename K, typename V, class RecManager>
bool unbundled_graph<K, V, RecManager>::contains(const int tid, const K& key){
    recordmgr->leaveQuiescentState(tid, true);
    nodeptr curr;
    std::list<nodeptr> queue;
    queue.push_back(head);
    V res = NO_VALUE;
    bool check = false;
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr && (curr->key == key) && !curr->marked){
            res = curr->val;
            goto endOfTheLoop;
        }
        if(curr){
                 for(auto& x : curr->neighbors){
                    if(!x->visited){
                        (x)->visited = true;
                        queue.push_back(x);
                    }
                    if(((x)->key == key) && !(x)->marked){
                        res = (x)->val;
                        goto endOfTheLoop;
                    }
                }
        }
           
    }
    endOfTheLoop:
        if(res!= NO_VALUE){
            check = true;
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
V unbundled_graph<K, V, RecManager>::doInsert(const int tid, const K& key,
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
                "unbundled_graph_bundled at this time."
             << endl;
            exit(-1);
        }
        assert(curr->key != key);
        result = NO_VALUE;
        newnode = new_node(tid, key, val, nullptr);
        // curr->neighbors.emplace_back(newnode);
        nodeptr insertedNodes[] = {newnode, NULL};
        nodeptr deletedNodes[] = {NULL};

        // Perform original linearization.
        rqProvider->linearize_update_at_write_for_unbundled_graphs(tid, &pred, newnode, &newnode, curr, insertedNodes, deletedNodes);
    
        releaseLock(&(curr->lock));
        releaseLock(&(pred->lock));
        recordmgr->enterQuiescentState(tid);
        return result;
    }
}

template<typename K, typename V, class RecManager>
void unbundled_graph<K, V, RecManager>::eraseNeighbors(nodeptr node, const K& key){
    for(auto& x : node->neighbors){
        if(x->key == key){
            acquireLock(&(node->lock));
            node->neighbors.erase(std::remove(node->neighbors.begin(), node->neighbors.end(),x), node->neighbors.end());
            releaseLock(&(node->lock));
            break;
        }
    }
}
template<typename K, typename V, class RecManager>
V unbundled_graph<K, V, RecManager>::erase(const int tid, const K& key){
    nodeptr pred;
    nodeptr curr;
    V result = NO_VALUE;
    std::list<nodeptr> queue;
    while(true){
        recordmgr->leaveQuiescentState(tid);
        for(auto& u : totalNodes){
            if(u->key == key){
                
                acquireLock(&(u->lock));
                 nodeptr insertedNodes[] = {NULL};
                nodeptr deletedNodes[] = {u, NULL};
                result = u->val;
                rqProvider->linearize_update_at_write(tid, &u->marked, 1LL, insertedNodes, deletedNodes);
                u->neighbors.clear();
                 rqProvider->announce_physical_deletion(tid, deletedNodes);
                rqProvider->physical_deletion_succeeded(tid, deletedNodes);
                vectorLock.lock();
                totalNodes.erase(std::remove(totalNodes.begin(), totalNodes.end(), u), totalNodes.end());
                vectorLock.unlock();
                releaseLock(&(u->lock));
            }
            else{
                eraseNeighbors(u, key);
            }
        }


        recordmgr->enterQuiescentState(tid);
        return result;
    }

}

template<typename K, typename V, class RecManager>
int unbundled_graph<K, V, RecManager>::rangeQuery(const int tid, const K& lo,
                                        const K& hi, 
                                        K* const resultKeys,
                                        V* const resultValues){

    int cnt = 0;
    std::list<nodeptr> queue;
    nodeptr curr;
    recordmgr->leaveQuiescentState(tid, true);
    rqProvider->traversal_start(tid);
    queue.push_back(head);
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr->key <= hi && curr->key >= lo && !curr->marked){
            rqProvider->traversal_try_add(tid, curr, resultKeys, resultValues, &cnt, lo, hi);
        }
        for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                if(!(*x)->visited){
                        (*x)->visited = true;
                        queue.push_back(*x);
                }
        }
    }
    for(auto& u : totalNodes){
      if(u){
        u->visited = false;
      }
    }
    rqProvider->traversal_end(tid, resultKeys, resultValues, &cnt, lo, hi);
    recordmgr->enterQuiescentState(tid);
    return cnt;
}


template <typename K, typename V, class RecManager>
long long unbundled_graph<K, V, RecManager>::debugKeySum(nodeptr head){
    long long result = 0;
    nodeptr curr;
    std::list<nodeptr> queue;
    queue.push_back(head);
    while(!queue.empty()){
        curr = queue.front();
        queue.pop_front();
        if(curr->key < KEY_MAX){
            result += curr->key;
        }
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!(*x)->visited){
                        (*x)->visited = true;
                        queue.push_back(*x);
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
long long unbundled_graph<K, V, RecManager>::debugKeySum(){
    return debugKeySum(head);
}


template <typename K, typename V, class RecManager>
inline bool unbundled_graph<K, V, RecManager>::isLogicallyDeleted(const int tid, node_t<K, V>* node){
    return node->isMarked(tid, rqProvider);
}
#endif


