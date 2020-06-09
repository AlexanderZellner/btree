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
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];
        /// The values.
        uint64_t children[kCapacity];

        /// Constructor.
        InnerNode() : Node(0, 0) {}

        /// Returns whether the inner node is full.
        bool is_full() {
            return this->count == kCapacity;
        }

        /// Get the index of the first key that is not less than than a provided key.
        /// @param[in] key          The key that should be inserted.
        std::pair<uint32_t, bool> lower_bound(const KeyT&) {
            throw std::logic_error{"not implemented"};
        }

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] separator    The child that should be inserted.
        void insert(const KeyT&, uint64_t) {
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

        /// Returns the child page ids.
        std::vector<uint64_t> get_child_vector() {
            throw std::logic_error{"not implemented"};
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
