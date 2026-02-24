#include "sql/parser.h"

namespace minisql::sql
{

  bool Parser::match(TokenKind k)
  {
    if (cur_.kind == k)
    {
      advance();
      return true;
    }
    return false;
  }

  void Parser::expect(TokenKind k, const char *msg)
  {
    if (cur_.kind != k)
      throw std::runtime_error(msg);
    advance();
  }

  std::string Parser::parseIdent()
  {
    if (cur_.kind != TokenKind::Identifier)
      throw std::runtime_error("expected identifier");
    auto s = cur_.text;
    advance();
    return s;
  }

  Value Parser::parseValue()
  {
    if (cur_.kind == TokenKind::Number)
    {
      auto s = cur_.text;
      advance();
      return static_cast<int64_t>(std::stoll(s));
    }
    if (cur_.kind == TokenKind::String)
    {
      auto s = cur_.text;
      advance();
      return s;
    }
    throw std::runtime_error("expected value (number or string)");
  }

  DataType Parser::parseType()
  {
    if (match(TokenKind::KW_INT))
      return DataType::Int;
    if (match(TokenKind::KW_TEXT))
      return DataType::Text;
    throw std::runtime_error("expected type (INT or TEXT)");
  }

  CreateTableStmt Parser::parseCreateTable()
  {
    expect(TokenKind::KW_CREATE, "expected CREATE");
    expect(TokenKind::KW_TABLE, "expected TABLE");
    CreateTableStmt stmt;
    stmt.table_name = parseIdent();

    expect(TokenKind::LParen, "expected '(' after table name");

    while (true)
    {
      ColumnDef col;
      col.name = parseIdent();
      col.type = parseType();
      if (match(TokenKind::KW_PRIMARY))
      {
        expect(TokenKind::KW_KEY, "expected KEY after PRIMARY");
        col.primary_key = true;
      }
      stmt.columns.push_back(std::move(col));

      if (match(TokenKind::Comma))
        continue;
      break;
    }

    expect(TokenKind::RParen, "expected ')'");
    match(TokenKind::Semicolon);
    return stmt;
  }

  InsertStmt Parser::parseInsert()
  {
    expect(TokenKind::KW_INSERT, "expected INSERT");
    expect(TokenKind::KW_INTO, "expected INTO");

    InsertStmt stmt;
    stmt.table_name = parseIdent();

    expect(TokenKind::LParen, "expected '(' after table name");
    while (true)
    {
      stmt.columns.push_back(parseIdent());
      if (match(TokenKind::Comma))
        continue;
      break;
    }
    expect(TokenKind::RParen, "expected ')' after column list");

    expect(TokenKind::KW_VALUES, "expected VALUES");
    expect(TokenKind::LParen, "expected '(' after VALUES");

    while (true)
    {
      stmt.values.push_back(parseValue());
      if (match(TokenKind::Comma))
        continue;
      break;
    }
    expect(TokenKind::RParen, "expected ')' after values");
    match(TokenKind::Semicolon);
    return stmt;
  }

  SelectStmt Parser::parseSelect()
  {
    expect(TokenKind::KW_SELECT, "expected SELECT");

    SelectStmt stmt;

    if (match(TokenKind::Star))
    {
      stmt.columns.push_back("*");
    }
    else
    {
      while (true)
      {
        stmt.columns.push_back(parseIdent());
        if (match(TokenKind::Comma))
          continue;
        break;
      }
    }

    expect(TokenKind::KW_FROM, "expected FROM");
    stmt.table_name = parseIdent();

    if (match(TokenKind::KW_WHERE))
    {
      ExprEq eq;
      eq.column = parseIdent();
      expect(TokenKind::Equal, "expected '=' in WHERE");
      eq.value = parseValue();
      stmt.where_eq = eq;
    }

    match(TokenKind::Semicolon);
    return stmt;
  }

  ExplainStmt Parser::parseExplain()
  {
    expect(TokenKind::KW_EXPLAIN, "expected EXPLAIN");
    ExplainStmt ex;
    ex.select = parseSelect();
    return ex;
  }

  // DELETE FROM table [WHERE col = val];
  DeleteStmt Parser::parseDelete()
  {
    expect(TokenKind::KW_DELETE, "expected DELETE");
    expect(TokenKind::KW_FROM, "expected FROM after DELETE");
    DeleteStmt stmt;
    stmt.table_name = parseIdent();
    if (match(TokenKind::KW_WHERE))
    {
      ExprEq eq;
      eq.column = parseIdent();
      expect(TokenKind::Equal, "expected '=' in WHERE");
      eq.value = parseValue();
      stmt.where_eq = eq;
    }
    match(TokenKind::Semicolon);
    return stmt;
  }

  // UPDATE table SET col = val [WHERE col = val];
  UpdateStmt Parser::parseUpdate()
  {
    expect(TokenKind::KW_UPDATE, "expected UPDATE");
    UpdateStmt stmt;
    stmt.table_name = parseIdent();
    expect(TokenKind::KW_SET, "expected SET after table name");
    stmt.set_column = parseIdent();
    expect(TokenKind::Equal, "expected '=' after SET column");
    stmt.set_value = parseValue();
    if (match(TokenKind::KW_WHERE))
    {
      ExprEq eq;
      eq.column = parseIdent();
      expect(TokenKind::Equal, "expected '=' in WHERE");
      eq.value = parseValue();
      stmt.where_eq = eq;
    }
    match(TokenKind::Semicolon);
    return stmt;
  }

  Stmt Parser::parseStatement()
  {
    if (match(TokenKind::KW_BEGIN))
    {
      match(TokenKind::Semicolon);
      return {StmtKind::Begin, std::monostate{}};
    }
    if (match(TokenKind::KW_COMMIT))
    {
      match(TokenKind::Semicolon);
      return {StmtKind::Commit, std::monostate{}};
    }
    if (match(TokenKind::KW_ROLLBACK))
    {
      match(TokenKind::Semicolon);
      return {StmtKind::Rollback, std::monostate{}};
    }

    if (cur_.kind == TokenKind::KW_CREATE)
    {
      auto node = parseCreateTable();
      return {StmtKind::CreateTable, node};
    }
    if (cur_.kind == TokenKind::KW_INSERT)
    {
      auto node = parseInsert();
      return {StmtKind::Insert, node};
    }
    if (cur_.kind == TokenKind::KW_SELECT)
    {
      auto node = parseSelect();
      return {StmtKind::Select, node};
    }
    if (cur_.kind == TokenKind::KW_EXPLAIN)
    {
      auto node = parseExplain();
      return {StmtKind::Explain, node};
    }
    if (cur_.kind == TokenKind::KW_DELETE)
    {
      auto node = parseDelete();
      return {StmtKind::Delete, node};
    }
    if (cur_.kind == TokenKind::KW_UPDATE)
    {
      auto node = parseUpdate();
      return {StmtKind::Update, node};
    }

    throw std::runtime_error("unknown/unsupported statement");
  }

} // namespace minisql::sql