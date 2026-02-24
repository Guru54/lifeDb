#pragma once
#include <cctype>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace minisql::sql
{

  using namespace std;

  enum class TokenKind
  {
    End,
    Identifier,
    Number,
    String,

    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Equal,

    KW_CREATE,
    KW_TABLE,
    KW_INSERT,
    KW_INTO,
    KW_VALUES,
    KW_SELECT,
    KW_FROM,
    KW_WHERE,
    KW_PRIMARY,
    KW_KEY,
    KW_INT,
    KW_TEXT,
    KW_BEGIN,
    KW_COMMIT,
    KW_ROLLBACK,
    KW_EXPLAIN,
    KW_DELETE,
    KW_UPDATE,
    KW_SET,
    KW_SHOW,
    KW_TABLES
  };

  struct Token
  {
    TokenKind kind = TokenKind::End;
    string text;
  };

  class Lexer
  {
  public:
    explicit Lexer(string_view input) : input_(input) {}
    Token next();

  private:
    char peek() const { return (pos_ < input_.size()) ? input_[pos_] : '\0'; }
    char get() { return (pos_ < input_.size()) ? input_[pos_++] : '\0'; }
    void skip_ws();

    Token lex_identifier_or_keyword();
    Token lex_number();
    Token lex_string();

    static bool ieq(string_view a, string_view b);

  private:
    string_view input_;
    size_t pos_ = 0;
  };

} // namespace minisql::sql