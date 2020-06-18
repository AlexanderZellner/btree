#ifndef INCLUDE_MODERNDBS_BTREE_H
#define INCLUDE_MODERNDBS_BTREE_H

#include <cstddef>
#include <functional>
#include <optional>
#include "moderndbs/buffer_manager.h"
#include "moderndbs/defer.h"
#include "moderndbs/segment.h"

namespace moderndbs {

template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
    struct Node {
        /// The level in the tree.
        uint16_t level;
        /// The number of children.
        uint16_t count;

        // Constructor
        Node(uint16_t level, uint16_t count)
            : level(level), count(count) {}

        /// Is the node a leaf node?
        bool is_leaf() const { return level == 0; }
        /// Get the data of a node.
        std::byte* get_data() { return reinterpret_cast<std::byte*>(this); }
    };

    struct InnerNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have
        static constexpr uint32_t kCapacity = PageSize / sizeof(KeyT) + sizeof(ValueT) - 2;

        /// The keys.
        KeyT keys[kCapacity];
        /// The values.
        uint64_t children[kCapacity + 1];

        /// Constructor.
        InnerNode() : Node(0, 0) {}

        /// Returns whether the inner node is full.
        bool is_full() {
            return this->count == kCapacity;
        }

        /// Get the index of the first key that is not less than than a provided key.
        /// @param[in] key          The key that should be inserted.
        std::pair<uint32_t, bool> lower_bound(const KeyT& keyT) {
            // Binary search
            std::pair<uint32_t, bool> result;
            int first = 0;
            // length of arrray
            int last = this->count - 1;
            std::optional<uint32_t> index = {};
            assert(last < kCapacity);
            if (last == 1 && keys[first] > keyT) {
                result.first = first;
                result.second = true;
                return result;
            }

            while (first <= last) {
                int middle = first + (last - first) / 2;

                if (keyT == keys[middle]) {
                    result.first = middle;
                    result.second = true;
                    return result;
                }

                if (keyT > keys[middle]) {
                    first = middle + 1;
                } else {
                    index = middle;
                    last = middle - 1;
                }
            }

            if(index) {
                result.first = index.value();
                result.second = true;
            } else {
                result.first = 0;
                result.second = false;
            }
            return result;
        }

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] separator    The child that should be inserted.
        void insert(const KeyT& keyT, uint64_t child) {
            // insert key in keys[] -> insert child in children (page id)
            auto lower_bound = this->lower_bound(keyT);
            //vectorify arrays to insert key and child at correct point
            std::vector<KeyT> keys_vector = get_key_vector();
            std::vector<uint64_t> children_vector = get_child_vector();

            if (lower_bound.second) {
                keys_vector.insert(keys_vector.begin() + lower_bound.first, keyT);
                /// TODO: Check tree side
                children_vector.insert(children_vector.begin() + lower_bound.first, child);
            } else {
                // keyT is greater than all existing keys
                /// TODO: check split needed
                keys_vector.push_back(keyT);
                children_vector.push_back(child);
            }
            this->count++;

            // Copy vectos back to array
            keys = keys_vector.data();
            children = children_vector.data();

            assert(keys[lower_bound] == keys_vector.at(lower_bound));
            assert(children[lower_bound] == children_vector.at(lower_bound));
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // node must be full
            assert(*(&keys + 1) - keys == kCapacity);
            auto middle = (kCapacity - 1)  / 2;
            InnerNode* new_inner_node = reinterpret_cast<InnerNode*>(buffer);

            // get seperator key
            KeyT seperator_key = keys[middle];

            KeyT tmp_keys[kCapacity];
            uint64_t tmp_children[kCapacity + 1];
            // copy one half to new node
            for (int j = middle; j < kCapacity; ++j) {
                tmp_keys = keys[j];
                tmp_children = children[j];
                keys[j] = {};
                children[j] = {};
            }

            new_inner_node->keys = tmp_keys;
            new_inner_node->children = tmp_children;
            
            return seperator_key;
        }

        /// Returns the keys.
        std::vector<KeyT> get_key_vector() {
            std::vector<KeyT> keys_vector(std::begin(keys), std::end(keys));
            return keys_vector;

        }

        /// Returns the child page ids.
        std::vector<uint64_t> get_child_vector() {
            std::vector<uint64_t> children_vector(std::begin(children), std::end(children));
            return children_vector;
        }
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];
        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}

        /// Get the index of the first key that is not less than than a provided key.
        std::pair<uint32_t, bool> lower_bound(const KeyT&) {
            throw std::logic_error{"not implemented"};
        }

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT &, const ValueT &) {
            throw std::logic_error{"not implemented"};
        }

        /// Erase a key.
        void erase(const KeyT&) {
            throw std::logic_error{"not implemented"};
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte*) {
            throw std::logic_error{"not implemented"};
        }

        /// Returns the keys.
        std::vector<KeyT> get_key_vector() {
            throw std::logic_error{"not implemented"};
        }

        /// Returns the values.
        std::vector<ValueT> get_value_vector() {
            throw std::logic_error{"not implemented"};
        }
    };

    /// The root.
    std::optional<uint64_t> root;
    /// Next page id.
    uint64_t next_page_id;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager& buffer_manager)
        : Segment(segment_id, buffer_manager) {
        throw std::logic_error{"not implemented"};
    }
    /// Destructor.
    ~BTree() = default;

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    /// @return             Whether the key was in the tree.
    std::optional<ValueT> lookup(const KeyT&) {
        throw std::logic_error{"not implemented"};
    }

    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT&) {
        throw std::logic_error{"not implemented"};
    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT&, const ValueT&) {
        throw std::logic_error{"not implemented"};
    }
};

}  // namespace moderndbs

#endif
