#ifndef UNBUNDLED_GRAPH_H
#define UNBUNDLED_GRAPH_H

#ifndef MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY
// define BEFORE including rq_provider.h
    #define MAX_NODES_INSERTED_OR_DELETED_ATOMICALLY 4
#endif
#include <stack>
#include <unordered_set>
#include <algorithm>
#include <list>
#include <mutex>
#include <vector>

#include "rq_provider.h"
#include "unbundled_graph_impl.h"

template <typename K, typename V>
class node_t;
#define nodeptr node_t<K, V>*


template <typename K, typename V, class RecManager>

class unbundled_graph{
    private:
        RecManager* const recordmgr;
        RQProvider<K, V, node_t<K, V>, unbundled_graph<K, V, RecManager>, RecManager,
             true, false>* const rqProvider;
#ifdef USE_DEBUGCOUNTERS
        debugCounters* const counters;
#endif
        nodeptr head;
        std::vector<nodeptr> totalNodes;
        std::mutex vectorLock;
        nodeptr new_node(const int tid, const K& key, const V& val, nodeptr adjNode);
        long long debugKeySum(nodeptr head);

        V doInsert(const int tid, const K& key, const V& value, bool onlyIfAbsent);
        void eraseNeighbors(nodeptr node, const K& key);

        int init[MAX_TID_POW2] = {
            0,
        };
    
    public:
        const K KEY_MIN;
        const K KEY_MAX;
        const V NO_VALUE;

        unbundled_graph(int numProcesses, const K _KEY_MIN, const K _KEY_MAX, const V NO_VALUE);
        ~unbundled_graph();

        bool contains(const int tid, const K& key);
        V insert(const int tid, const K& key, const V& value) {
            return doInsert(tid, key, value, false);
        }
        V insertIfAbsent(const int tid, const K& key, const V& value) {
            return doInsert(tid, key, value, true);
        }
        V erase(const int tid, const K& key);
        int rangeQuery(const int tid, const K& lo, const K& hi, K* const resultKeys,
                 V* const resultValues);
        void cleanup(int tid);
        void startCleanup() { rqProvider->startCleanup(); }
        void stopCleanup() { rqProvider->stopCleanup(); }
        bool validateBundles(int tid);

          /**
   * This function must be called once by each thread that will
   * invoke any functions on this class.
   *
   * It must be okay that we do this with the main thread and later with another
   * thread!!!
   */
        void initThread(const int tid);
        void deinitThread(const int tid);
#ifdef USE_DEBUGCOUNTERS
        debugCounters* debugGetCounters() { return counters; }
        void clearCounters() { counters->clear(); }
#endif
        long long debugKeySum();
        bool validate(const long long keysum, const bool checkkeysum) {
            return true;
        }
        long long getSizeInNodes() {
            std::list<nodeptr> queue;
            queue.push_back(head);
            std::vector<nodeptr> revert_visited;
            long long size = 0;
            head->visited = true;
            nodeptr curr;
            while(!queue.empty()){
                curr = queue.front();
                queue.pop_front();
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!(*x)->visited){
                        (*x)->visited = true;
                        queue.push_back(*x); 
                    }
                }
                revert_visited.push_back(curr);
                ++size;
            }
            for(auto& u: revert_visited){
                if(u){
                    u->visited = false;
                }
            }
            return size;
        }
        long long getSize() {
            std::list<nodeptr> queue;
            queue.push_back(head);
            std::vector<nodeptr> revert_visited;
            long long size = 0;
            head->visited = true;
            nodeptr curr;
            while(!queue.empty()){
                curr = queue.front();
                queue.pop_front();
                for(auto x = curr->neighbors.begin(); x != curr->neighbors.end(); x++){
                    if(!(*x)->visited){
                        (*x)->visited = true;
                        queue.push_back(*x); 
                    }
                }
                revert_visited.push_back(curr);
                size += (!curr->marked);
            }
            for(auto& u: revert_visited){
                if(u){
                    u->visited = false;
                }
            }
            return size;
        }
          string getSizeString() {
            stringstream ss;
            ss<<getSizeInNodes()<<" nodes in data structure";
            return ss.str();
          }

        RecManager* const debugGetRecMgr() { return recordmgr; }

        inline int getKeys(const int tid, node_t<K, V>* node, K* const outputKeys,
                     V* const outputValues) {
            // ignore marked
            outputKeys[0] = node->key;
            outputValues[0] = node->val;
            return 1;
        }
        bool isInRange(const K& key, const K& lo, const K& hi) {
            return (lo <= key && key <= hi);
        }
        inline bool isLogicallyDeleted(const int tid, node_t<K, V>* node);

        inline bool isLogicallyInserted(const int tid, node_t<K, V>* node) {
            return true;
        }

        node_t<K, V>* debug_getEntryPoint() { return head; }
        
};

#endif /* GRAPH_H */