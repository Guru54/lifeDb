cmake_minimum_required(VERSION 3.20)
project(minisql LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(minisql
  src/main.cpp
  src/sql/lexer.cpp
  src/sql/parser.cpp
  src/engine/catalog.cpp
  src/engine/row_codec.cpp
  src/engine/pager.cpp
  src/engine/executor.cpp
)

target_include_directories(minisql PRIVATE src)