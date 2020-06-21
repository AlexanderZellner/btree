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
        static constexpr uint32_t kCapacity = PageSize / (sizeof(KeyT) + sizeof(ValueT)) - 2;

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
            // node is empty
            if (last < first) {
                result.first = first;
                result.second = false;
                return result;
            }

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

            if (this->count == 1) {
                // insert only child if only one key
                children_vector.push_back(child);
                this->count++;
                for (auto i = 0; i < this->count; ++i) {
                    children[i] = children_vector.at(i);
                }
                return;
            } else {
                if (lower_bound.second) {
                    keys_vector.insert(keys_vector.begin() + lower_bound.first, keyT);
                    if (lower_bound.first < this->count - 1) {
                        if(keyT > keys[lower_bound.first]) {
                            //existing key
                            children_vector.at(lower_bound.first) = child;
                            // write back vector
                            for (unsigned long i = 0; i < this->count; ++i) {
                                if (i < keys_vector.size()) {
                                    keys[i] = keys_vector.at(i);
                                }
                                children[i] = children_vector.at(i);
                            }
                            return;
                        } else {
                            // inserting in lhs -> new page > old page
                            lower_bound.first += 1;
                        }
                    }
                    children_vector.insert(children_vector.begin() + lower_bound.first, child);
                } else {
                    // keyT is greater than all existing keys
                    keys_vector.push_back(keyT);
                    children_vector.push_back(child);
                }
            }
            this->count++;

            // Copy vectors back to array
            for (unsigned long i = 0; i < this->count; ++i) {
               if (i < keys_vector.size()) {
                   keys[i] = keys_vector.at(i);
               }
                children[i] = children_vector.at(i);
            }
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // node must be full
            assert(this->count == kCapacity);
            auto middle = this->count  / 2;
            auto* new_inner_node = reinterpret_cast<InnerNode*>(buffer);

            this->count -= middle;
            new_inner_node->count = middle;
            new_inner_node->level = this->level;
            assert(this->level > 0);

            memcpy(new_inner_node->keys, keys+this->count, (middle - 1) * sizeof(KeyT));
            memcpy(new_inner_node->children, children+this->count, middle * sizeof(uint64_t));

            // get seperator key
            KeyT seperator_key = keys[this->count - 1];

            return seperator_key;
        }

        /// Returns the keys.
        std::vector<KeyT> get_key_vector() {
            uint16_t num_keys = this->count == 0 ? this->count : this->count - 1;
            std::vector<KeyT> keys_vector(keys, keys + num_keys);
            return keys_vector;

        }

        /// Returns the child page ids.
        std::vector<uint64_t> get_child_vector() {
            std::vector<uint64_t> children_vector(children, children + this->count);
            return children_vector;
        }
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        static constexpr uint32_t kCapacity = (PageSize / (sizeof(KeyT) + sizeof(ValueT))) - 2;

        /// The keys.
        KeyT keys[kCapacity];
        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}

        bool is_full() {
            return this->count == kCapacity;
        }

        /// Get the index of the first key that is not less than than a provided key.
        std::pair<uint32_t, bool> lower_bound(const KeyT& keyT) {
            // Binary search
            std::pair<uint32_t, bool> result;
            int first = 0;
            // length of arrray
            int last = this->count - 1;
            std::optional<uint32_t> index = {};
            // node is empty
            if (last < first) {
                result.first = first;
                result.second = false;
                return result;
            }
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
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT & keyT, const ValueT & valueT) {
            // insert key in keys[] -> insert child in children (page id)
            auto lower_bound = this->lower_bound(keyT);
            //vectorify arrays to insert key and child at correct point
            std::vector<KeyT> keys_vector = get_key_vector();
            std::vector<ValueT> values_vector = get_value_vector();

            if (lower_bound.second) {
                if (keyT >= keys[lower_bound.first]) {
                    // update value for existing key
                    values_vector.at(lower_bound.first) = valueT;
                    for (auto i = 0; i < this->count; ++i) {
                        keys[i] = keys_vector.at(i);
                        values[i] = values_vector.at(i);
                    }
                    return;
                }
                keys_vector.insert(keys_vector.begin() + lower_bound.first, keyT);
                values_vector.insert(values_vector.begin() + lower_bound.first, valueT);
            } else {
                // keyT is greater than all existing keys
                keys_vector.insert(keys_vector.begin() + this->count, keyT);
                values_vector.insert(values_vector.begin() + this->count, valueT);
            }
            this->count++;

            // Copy vectors back to array
            for (auto i = 0; i < this->count; ++i) {
                keys[i] = keys_vector.at(i);
                values[i] = values_vector.at(i);
            }
        }

        /// Erase a key.
        void erase(const KeyT& keyT) {
            auto lower_bound = this->lower_bound(keyT);
            uint32_t i = lower_bound.first;
            if (keys[lower_bound.first] == keyT) {
                memmove(keys + i, keys + i + 1, (this->count - i - 1) * sizeof(KeyT));
                memmove(values + i, values + i + 1, (this->count - i - 1) * sizeof(ValueT));

                assert(this->count >= 0);
                this->count--;
            }
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            assert(this->count == kCapacity);
            auto middle = (this->count - 1) / 2;
            auto* new_leaf_node = reinterpret_cast<LeafNode*>(buffer);

            this->count -= middle;
            new_leaf_node->count = middle;

            auto seperatorKey = keys[middle + 1];

            auto* start_key = keys + this->count;
            auto* start_value = values + this->count;
            std::copy(start_key, start_key + middle, new_leaf_node->keys);
            std::copy(start_value, start_value + middle + 1, new_leaf_node->values);

            return seperatorKey;
        }

        /// Returns the keys.
        std::vector<KeyT> get_key_vector() {
            std::vector<KeyT> keys_vector(keys, keys + this->count);
            return keys_vector;
        }

        /// Returns the values.
        std::vector<ValueT> get_value_vector() {
            std::vector<ValueT> values_vector(values, values + this->count);
            return values_vector;
        }
    };

    /// The root.
    std::optional<uint64_t> root;
    /// Next page id.
    uint64_t next_page_id;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager& buffer_manager)
        : Segment(segment_id, buffer_manager) {

    }
    /// Destructor.
    ~BTree() = default;

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    /// @return             Whether the key was in the tree.
    std::optional<ValueT> lookup(const KeyT& keyT) {
        std::optional<ValueT> foundKey;

        if (!root) {
            // empty tree
            return foundKey;
        }

        uint64_t next_page;
        BufferFrame* bufferFrame = &buffer_manager.fix_page(root.value(), false);
        auto* node = reinterpret_cast<Node*>(bufferFrame->get_data());

        while(!node->is_leaf()) {
            // inner node
            auto* innerNode = reinterpret_cast<InnerNode*>(node);
            // first keys[index] > keyT -> index - 1 ==> next_page
            auto lowerBound = innerNode->lower_bound(keyT);
            uint32_t child_index = 0;
            if (lowerBound.second) {
                // was - 1 here
                child_index = lowerBound.first;
            } else {
                // keyT > all other keys -> must be in the most right branch
                child_index = node->count - 1;
            }

            next_page = innerNode->children[child_index];

            // lock coupling
            BufferFrame* bufferFrame_child = &buffer_manager.fix_page(next_page, false);
            buffer_manager.unfix_page(*bufferFrame, false);
            bufferFrame = bufferFrame_child;

            node = reinterpret_cast<Node*>(bufferFrame->get_data());
        }
        // node is leaf
        auto* leafNode = reinterpret_cast<LeafNode*>(node);
        auto lowerBound = leafNode->lower_bound(keyT);
        uint32_t value_index = lowerBound.first;
        std::optional<ValueT> value;

        if (lowerBound.second) {
            // first > KeyT => -1
            if (leafNode->keys[value_index] == keyT) {
                value = leafNode->values[value_index];
            }
        } else {
            // all found keys were smaller than keyT => no result
            value = {};
        }

        buffer_manager.unfix_page(*bufferFrame, false);
        return value;
    }

    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT& keyT) {
        if (!root) {
            // cant't delete from empty tree
            return;
        }

        uint64_t next_page;
        BufferFrame* bufferFrame = &buffer_manager.fix_page(root.value(), false);
        auto* node = reinterpret_cast<Node*>(bufferFrame->get_data());

        while(!node->is_leaf()) {
            // inner node
            auto* innerNode = reinterpret_cast<InnerNode*>(node);
            // first keys[index] > keyT -> index - 1 ==> next_page
            auto lowerBound = innerNode->lower_bound(keyT);
            uint32_t child_index = 0;
            if (lowerBound.second) {
                // was - 1 here
                child_index = lowerBound.first;
            } else {
                // keyT > all other keys -> must be in the most right branch
                child_index = node->count - 1;
            }

            next_page = innerNode->children[child_index];

            BufferFrame* bufferFrame_child = &buffer_manager.fix_page(next_page, false);
            buffer_manager.unfix_page(*bufferFrame, false);
            bufferFrame = bufferFrame_child;

            node = reinterpret_cast<Node*>(bufferFrame->get_data());
        }
        // node is leaf
        auto* leafNode = reinterpret_cast<LeafNode*>(node);

        // remove leaf if node == empty -> delete node
        leafNode->erase(keyT);

    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT& keyT, const ValueT& valueT) {
        if (!root) {
            // empty tree
            root = 0;
            next_page_id = 1;
        }

        // current node to operate on
        BufferFrame* bufferFrame = &buffer_manager.fix_page(root.value(), true);
        BufferFrame* bufferFrame_parent;
        bool isDirty = false;
        bool isDirty_parent = false;
        bool onRoot = true;

        uint64_t parent_page_id = root.value();
        // "safe" inner pages = split when node is full
        while(true) {
            auto* node = reinterpret_cast<Node*>(bufferFrame->get_data());

            if (node->is_leaf()) {
                auto* leafNode = reinterpret_cast<LeafNode*>(node);
                if (leafNode->is_full()) {
                    // leaf needs to be split
                    uint64_t new_leaf_id = next_page_id++;
                    BufferFrame* bufferFrame_new = &buffer_manager.fix_page(new_leaf_id, true);
                    KeyT separator = leafNode->split(reinterpret_cast<std::byte *>(bufferFrame_new->get_data()));
                    isDirty = true;

                    if (parent_page_id == 0) {
                        // only root node
                        uint64_t old_leaf_id = root.value();
                        root = next_page_id++;
                        parent_page_id = root.value();

                        bufferFrame_parent = &buffer_manager.fix_page(root.value(), true);
                        isDirty_parent = true;
                        // new root node
                        auto* innerNode = reinterpret_cast<InnerNode*>(bufferFrame_parent->get_data());

                        assert(!innerNode->is_full());
                        innerNode->level = 1;
                        innerNode->count = 0;

                        innerNode->insert(separator,old_leaf_id);
                        assert(innerNode->count == 1);
                        innerNode->insert(separator, new_leaf_id);
                        if (separator < keyT) {
                            buffer_manager.unfix_page(*bufferFrame, isDirty);
                            bufferFrame = bufferFrame_new;
                        }
                    } else {
                        // leaf is not root
                        auto* parent_node = reinterpret_cast<InnerNode*>(bufferFrame_parent->get_data());

                        if (keyT < separator) {
                            // key is on old page -> change new page and old page index
                            //parent_node->insert(separator, current_id);

                        }
                        assert(!parent_node->is_full());
                        assert(parent_node->level >= 1);
                        parent_node->insert(separator, new_leaf_id);
                        isDirty = true;
                        isDirty_parent = true;

                        // new iteration with not full child
                        // hold lock parent and correct child
                        if (keyT < separator) {
                            buffer_manager.unfix_page(*bufferFrame_new, isDirty);
                        } else {
                            buffer_manager.unfix_page(*bufferFrame, isDirty);
                            bufferFrame = bufferFrame_new;
                        }
                    }
                } else {
                    // leaf is not full
                    leafNode->insert(keyT, valueT);
                    isDirty = true;

                    if(parent_page_id != 0) {
                        // leaf is not root -> has parent
                        if (bufferFrame_parent != nullptr) {
                            buffer_manager.unfix_page(*bufferFrame_parent, isDirty_parent);
                        }
                    }

                    buffer_manager.unfix_page(*bufferFrame, isDirty);
                    return;
                }

            } else {
                // node is inner node
                auto* innerNode = reinterpret_cast<InnerNode*>(node);
                if (innerNode->is_full()) {
                    assert(parent_page_id != 0);
                    // innerNode is full -> split
                    uint64_t new_innerNode_id = next_page_id++;
                    BufferFrame* bufferFrame_new = &buffer_manager.fix_page(new_innerNode_id, true);
                    KeyT separator = innerNode->split(reinterpret_cast<std::byte *>(bufferFrame_new->get_data()));
                    // get parent
                    if (onRoot) {
                        // inner node is root -> needs new parent
                        uint64_t old_root_id = root.value();
                        root = next_page_id++;
                        bufferFrame_parent = &buffer_manager.fix_page(root.value(), true);
                        auto* parent_node = reinterpret_cast<InnerNode*>(bufferFrame_parent->get_data());

                        parent_node->level = innerNode->level + 1;

                        parent_node->insert(separator, old_root_id);
                        assert(parent_node->count == 1);
                        parent_node->insert(separator, new_innerNode_id);
                        isDirty = true;

                        if (separator < keyT) {
                            buffer_manager.unfix_page(*bufferFrame, isDirty);
                            bufferFrame = bufferFrame_new;
                        }
                    } else {
                        auto* parent_node = reinterpret_cast<InnerNode *>(bufferFrame_parent->get_data());

                        assert(!parent_node->is_full());
                        parent_node->level = innerNode->level + 1;
                        parent_node->insert(separator, new_innerNode_id);

                        isDirty = true;
                        isDirty_parent = true;

                        auto lowerBound = parent_node->lower_bound(keyT);

                        if (lowerBound.second) {
                            // not the last key -> unfix new bufferFrame
                            buffer_manager.unfix_page(*bufferFrame_new, isDirty);
                        } else {
                            buffer_manager.unfix_page(*bufferFrame, isDirty);
                            bufferFrame = bufferFrame_new;
                        }
                    }
                } else {
                    // innerNode has enough space
                    auto lowerBound = innerNode->lower_bound(keyT);
                    // new parent
                    if (!onRoot) {
                        if (bufferFrame_parent != nullptr) {
                            buffer_manager.unfix_page(*bufferFrame_parent, isDirty_parent);
                        }
                    }
                    bufferFrame_parent = bufferFrame;

                    if (lowerBound.second) {
                        // left child
                        uint64_t child_id = innerNode->children[lowerBound.first];
                        bufferFrame = &buffer_manager.fix_page(child_id, true);
                    } else {
                        // right child
                        uint64_t child_id = innerNode->children[innerNode->count - 1];
                        bufferFrame = &buffer_manager.fix_page(child_id, true);
                    }
                }
            }
            // after first iteration node cannot be root anymore -> always a parent now
            onRoot = false;
        }
    }
};

}  // namespace moderndbs

#endif
