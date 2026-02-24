#pragma once
#include <string_view>
#include <stdexcept>

#include "sql/ast.h"
#include "sql/lexer.h"

namespace minisql::sql
{

  class Parser
  {
  public:
    explicit Parser(std::string_view input) : lex_(input) { advance(); }
    Stmt parseStatement();

  private:
    void advance() { cur_ = lex_.next(); }
    bool match(TokenKind k);
    void expect(TokenKind k, const char *msg);

    std::string parseIdent();
    Value parseValue();
    DataType parseType();

    CreateTableStmt parseCreateTable();
    InsertStmt parseInsert();
    SelectStmt parseSelect();
    ExplainStmt parseExplain();
    DeleteStmt parseDelete();
    UpdateStmt parseUpdate();

  private:
    Lexer lex_;
    Token cur_;
  };

} // namespace minisql::sql