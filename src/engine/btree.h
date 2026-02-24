#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <functional>
#include <optional>
#include <utility>

namespace minisql::engine {

static constexpr uint32_t BT_PAGE_SIZE   = 4096;
static constexpr int      BT_ORDER       = 50;   // max keys per node

// leaf entry: key + row location
struct BTreeEntry { int64_t key; uint32_t page_id; uint32_t row_off; };

class BTree {
public:
    explicit BTree(const std::string& filepath);
    ~BTree();

    void insert(int64_t key, uint32_t page_id, uint32_t row_off);
    bool find  (int64_t key, BTreeEntry& out) const;
    void scanAll(std::function<void(const BTreeEntry&)> cb) const;

private:
    static constexpr uint32_t BT_MAGIC        = 0x42545245;
    static constexpr int      INT_KEYS_OFF     = 7;
    static constexpr int      INT_CHILDREN_OFF = 407;  // 7 + 50*8

    struct Header    { uint32_t magic = BT_MAGIC; uint32_t root_page = 0; uint32_t num_pages = 1; };

    struct NodePage {
        bool     is_leaf   = true;
        int      num_keys  = 0;
        uint32_t next_leaf = 0;
        BTreeEntry entries[BT_ORDER + 1]{};   // leaf slots (over-size for split)
        int64_t    keys    [BT_ORDER    ]{};   // internal keys
        uint32_t   children[BT_ORDER + 1]{};  // internal children
    };

    NodePage readNode (uint32_t pid) const;
    void     writeNode(uint32_t pid, const NodePage& n);

    void     readHeader ();
    void     writeHeader();
    uint32_t allocPage  ();

    std::optional<std::pair<int64_t, uint32_t>>
    insertRec(uint32_t pid, int64_t key, uint32_t page_id, uint32_t row_off);

    mutable std::fstream file_;
    std::string          filepath_;
    Header               hdr_;
};

} // namespace minisql::engine
