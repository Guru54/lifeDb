#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <functional>

namespace minisql::engine {

static constexpr uint32_t PAGE_SIZE   = 4096;
static constexpr uint32_t PAGER_MAGIC = 0x4C51534D;

class Pager {
public:
    explicit Pager(const std::string& filepath);
    ~Pager();

    struct RowLocation { uint32_t page_id; uint32_t row_off; };

    RowLocation appendRow(const std::vector<uint8_t>& row_data);
    void scan(std::function<void(const uint8_t*, uint32_t)> cb);
    void scanEx(std::function<void(uint32_t, uint32_t, const uint8_t*, uint32_t)> cb);
    void markDeleted(uint32_t page_id, uint32_t row_off);
    std::vector<uint8_t> readPage(uint32_t page_id);
    uint32_t numRows() const { return hdr_.num_rows; }

private:
    static constexpr uint32_t DATA_CAPACITY = PAGE_SIZE - 2;

    struct Header {
        uint32_t magic        = PAGER_MAGIC;
        uint32_t num_pages    = 1;
        uint32_t num_rows     = 0;
        uint32_t write_page   = 0;
        uint16_t write_offset = 0;
    };

    void readHeader();
    void writeHeader();
    void writePage(uint32_t page_id, const std::vector<uint8_t>& data);
    uint32_t allocPage();

    std::fstream file_;
    std::string  filepath_;
    Header       hdr_;
};

} // namespace minisql::engine
