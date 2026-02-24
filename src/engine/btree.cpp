#include "engine/btree.h"

#include <cstring>
#include <stdexcept>
#include <cassert>

namespace minisql::engine {

using namespace std;

// ── File open/create ──────────────────────────────────────────────────────────
BTree::BTree(const string& filepath) : filepath_(filepath) {
    file_.open(filepath, ios::binary | ios::in | ios::out);
    if (!file_.is_open()) {
        { ofstream tmp(filepath, ios::binary | ios::trunc); }
        file_.open(filepath, ios::binary | ios::in | ios::out);
        if (!file_) throw runtime_error("btree: cannot open: " + filepath);
        hdr_ = Header{};
        writeHeader();
    } else {
        readHeader();
        if (hdr_.magic != BT_MAGIC)
            throw runtime_error("btree: bad magic in: " + filepath);
    }
}

BTree::~BTree() { if (file_.is_open()) file_.close(); }

// ── Header I/O ────────────────────────────────────────────────────────────────
void BTree::readHeader() {
    vector<uint8_t> pg(BT_PAGE_SIZE, 0);
    file_.clear();
    file_.seekg(0); file_.read(reinterpret_cast<char*>(pg.data()), BT_PAGE_SIZE);
    memcpy(&hdr_.magic,     pg.data() + 0, 4);
    memcpy(&hdr_.root_page, pg.data() + 4, 4);
    memcpy(&hdr_.num_pages, pg.data() + 8, 4);
}

void BTree::writeHeader() {
    vector<uint8_t> pg(BT_PAGE_SIZE, 0);
    memcpy(pg.data() + 0, &hdr_.magic,     4);
    memcpy(pg.data() + 4, &hdr_.root_page, 4);
    memcpy(pg.data() + 8, &hdr_.num_pages, 4);
    file_.seekp(0); file_.write(reinterpret_cast<char*>(pg.data()), BT_PAGE_SIZE);
    file_.flush();
}

// ── Page alloc ────────────────────────────────────────────────────────────────
uint32_t BTree::allocPage() {
    uint32_t pid = hdr_.num_pages++;
    vector<uint8_t> blank(BT_PAGE_SIZE, 0);
    file_.seekp(0, ios::end);
    file_.write(reinterpret_cast<char*>(blank.data()), BT_PAGE_SIZE);
    file_.flush();
    writeHeader();
    return pid;
}

// ── Node deserialise ──────────────────────────────────────────────────────────
BTree::NodePage BTree::readNode(uint32_t pid) const {
    vector<uint8_t> buf(BT_PAGE_SIZE, 0);
    file_.clear();
    file_.seekg(static_cast<streamoff>(pid) * BT_PAGE_SIZE);
    file_.read(reinterpret_cast<char*>(buf.data()), BT_PAGE_SIZE);

    NodePage n{};
    n.is_leaf  = (buf[0] == 1);
    memcpy(&n.num_keys,  buf.data() + 1, 2);
    memcpy(&n.next_leaf, buf.data() + 3, 4);

    if (n.is_leaf) {
        size_t off = 7;
        for (int i = 0; i < n.num_keys; i++, off += 16) {
            memcpy(&n.entries[i].key,     buf.data() + off,     8);
            memcpy(&n.entries[i].page_id, buf.data() + off + 8, 4);
            memcpy(&n.entries[i].row_off, buf.data() + off + 12, 4);
        }
    } else {
        // keys
        size_t off = INT_KEYS_OFF;
        for (int i = 0; i < n.num_keys; i++, off += 8)
            memcpy(&n.keys[i], buf.data() + off, 8);
        // children
        off = INT_CHILDREN_OFF;
        for (int i = 0; i <= n.num_keys; i++, off += 4)
            memcpy(&n.children[i], buf.data() + off, 4);
    }
    return n;
}

// ── Node serialise ────────────────────────────────────────────────────────────
void BTree::writeNode(uint32_t pid, const NodePage& n) {
    vector<uint8_t> buf(BT_PAGE_SIZE, 0);
    buf[0] = n.is_leaf ? 1 : 0;
    memcpy(buf.data() + 1, &n.num_keys,  2);
    memcpy(buf.data() + 3, &n.next_leaf, 4);

    if (n.is_leaf) {
        size_t off = 7;
        for (int i = 0; i < n.num_keys; i++, off += 16) {
            memcpy(buf.data() + off,      &n.entries[i].key,     8);
            memcpy(buf.data() + off + 8,  &n.entries[i].page_id, 4);
            memcpy(buf.data() + off + 12, &n.entries[i].row_off, 4);
        }
    } else {
        size_t off = INT_KEYS_OFF;
        for (int i = 0; i < n.num_keys; i++, off += 8)
            memcpy(buf.data() + off, &n.keys[i], 8);
        off = INT_CHILDREN_OFF;
        for (int i = 0; i <= n.num_keys; i++, off += 4)
            memcpy(buf.data() + off, &n.children[i], 4);
    }

    file_.seekp(static_cast<streamoff>(pid) * BT_PAGE_SIZE);
    file_.write(reinterpret_cast<char*>(buf.data()), BT_PAGE_SIZE);
    file_.flush();
}

// ── Recursive insert ──────────────────────────────────────────────────────────
optional<pair<int64_t, uint32_t>>
BTree::insertRec(uint32_t pid, int64_t key, uint32_t page_id, uint32_t row_off) {
    NodePage node = readNode(pid);

    if (node.is_leaf) {
        // Find insertion position
        int pos = 0;
        while (pos < node.num_keys && node.entries[pos].key < key) pos++;
        if (pos < node.num_keys && node.entries[pos].key == key)
            throw runtime_error("btree: duplicate primary key: " + to_string(key));

        // Shift right
        for (int i = node.num_keys; i > pos; i--) node.entries[i] = node.entries[i-1];
        node.entries[pos] = {key, page_id, row_off};
        node.num_keys++;

        if (node.num_keys <= BT_ORDER) {
            writeNode(pid, node);
            return nullopt;
        }

        // Split leaf
        int mid = (BT_ORDER + 1) / 2;
        uint32_t rpid = allocPage();
        NodePage right{};
        right.is_leaf   = true;
        right.num_keys  = node.num_keys - mid;
        right.next_leaf = node.next_leaf;
        for (int i = 0; i < right.num_keys; i++) right.entries[i] = node.entries[mid + i];
        node.num_keys  = mid;
        node.next_leaf = rpid;
        writeNode(pid, node);
        writeNode(rpid, right);
        return make_pair(right.entries[0].key, rpid);

    } else {
        // Internal: find child
        int i = 0;
        while (i < node.num_keys && key >= node.keys[i]) i++;
        auto res = insertRec(node.children[i], key, page_id, row_off);
        if (!res) return nullopt;

        auto [push_key, rchild] = *res;
        // Insert push_key at position i, rchild at i+1
        for (int j = node.num_keys; j > i; j--) {
            node.keys[j]         = node.keys[j-1];
            node.children[j + 1] = node.children[j];
        }
        node.keys[i]         = push_key;
        node.children[i + 1] = rchild;
        node.num_keys++;

        if (node.num_keys <= BT_ORDER) {
            writeNode(pid, node);
            return nullopt;
        }

        // Split internal
        int mid = BT_ORDER / 2;
        int64_t  up_key  = node.keys[mid];
        uint32_t rpid    = allocPage();
        NodePage right{};
        right.is_leaf  = false;
        right.num_keys = node.num_keys - mid - 1;
        for (int j = 0; j < right.num_keys; j++)    right.keys[j]     = node.keys[mid + 1 + j];
        for (int j = 0; j <= right.num_keys; j++)   right.children[j] = node.children[mid + 1 + j];
        node.num_keys = mid;
        writeNode(pid, node);
        writeNode(rpid, right);
        return make_pair(up_key, rpid);
    }
}

// ── Public insert ─────────────────────────────────────────────────────────────
void BTree::insert(int64_t key, uint32_t page_id, uint32_t row_off) {
    if (hdr_.root_page == 0) {
        // First insert: create root leaf
        uint32_t root = allocPage();
        hdr_.root_page = root;
        NodePage leaf{};
        leaf.is_leaf   = true;
        leaf.num_keys  = 1;
        leaf.entries[0] = {key, page_id, row_off};
        writeNode(root, leaf);
        writeHeader();
        return;
    }

    auto res = insertRec(hdr_.root_page, key, page_id, row_off);
    if (!res) return;

    // Root split: new root
    auto [up_key, rchild] = *res;
    uint32_t new_root = allocPage();
    NodePage root{};
    root.is_leaf      = false;
    root.num_keys     = 1;
    root.keys[0]      = up_key;
    root.children[0]  = hdr_.root_page;
    root.children[1]  = rchild;
    writeNode(new_root, root);
    hdr_.root_page = new_root;
    writeHeader();
}

// ── Public find ───────────────────────────────────────────────────────────────
bool BTree::find(int64_t key, BTreeEntry& out) const {
    if (hdr_.root_page == 0) return false;
    uint32_t pid = hdr_.root_page;
    while (true) {
        NodePage node = readNode(pid);
        if (node.is_leaf) {
            for (int i = 0; i < node.num_keys; i++) {
                if (node.entries[i].key == key) { out = node.entries[i]; return true; }
            }
            return false;
        }
        int i = 0;
        while (i < node.num_keys && key >= node.keys[i]) i++;
        pid = node.children[i];
    }
}

// ── Public scanAll (leaf chain) ───────────────────────────────────────────────
void BTree::scanAll(function<void(const BTreeEntry&)> cb) const {
    if (hdr_.root_page == 0) return;
    // Walk down to leftmost leaf
    uint32_t pid = hdr_.root_page;
    while (true) {
        NodePage n = readNode(pid);
        if (n.is_leaf) break;
        pid = n.children[0];
    }
    // Traverse leaf chain
    while (pid != 0) {
        NodePage leaf = readNode(pid);
        for (int i = 0; i < leaf.num_keys; i++) cb(leaf.entries[i]);
        pid = leaf.next_leaf;
    }
}

} // namespace minisql::engine
