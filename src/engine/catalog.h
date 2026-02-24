#pragma once
#include <string>
#include <vector>
#include <map>
#include "sql/ast.h"

namespace minisql::engine {

struct ColumnMeta {
    std::string            name;
    minisql::sql::DataType type;
    bool                   is_primary_key = false;
};

struct TableMeta {
    std::string             table_name;
    std::vector<ColumnMeta> columns;
};

class Catalog {
public:
    void       createTable(TableMeta meta);
    TableMeta* getTable(const std::string& name);
    void       saveToDisk(const std::string& filepath);
    void       loadFromDisk(const std::string& filepath);

    std::map<std::string, TableMeta> tables;
};

} // namespace minisql::engine
