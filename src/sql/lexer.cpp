#include "sql/lexer.h"

namespace minisql::sql
{

  using namespace std;

  void Lexer::skip_ws()
  {
    while (isspace(static_cast<unsigned char>(peek())))
      get();
  }

  bool Lexer::ieq(string_view a, string_view b)
  {
    if (a.size() != b.size())
      return false;
    for (size_t i = 0; i < a.size(); i++)
    {
      auto ca = static_cast<char>(toupper(static_cast<unsigned char>(a[i])));
      auto cb = static_cast<char>(toupper(static_cast<unsigned char>(b[i])));
      if (ca != cb)
        return false;
    }
    return true;
  }

  Token Lexer::lex_number()
  {
    string s;
    while (isdigit(static_cast<unsigned char>(peek())))
      s.push_back(get());
    return Token{TokenKind::Number, s};
  }

  Token Lexer::lex_string()
  {
    string s;
    while (true)
    {
      char c = get();
      if (c == '\0')
        throw runtime_error("unterminated string literal");
      if (c == '\'')
        break;
      s.push_back(c);
    }
    return Token{TokenKind::String, s};
  }

  Token Lexer::lex_identifier_or_keyword()
  {
    string s;
    while (true)
    {
      char c = peek();
      if (isalnum(static_cast<unsigned char>(c)) || c == '_')
        s.push_back(get());
      else
        break;
    }

    if (ieq(s, "CREATE"))
      return {TokenKind::KW_CREATE, s};
    if (ieq(s, "TABLE"))
      return {TokenKind::KW_TABLE, s};
    if (ieq(s, "INSERT"))
      return {TokenKind::KW_INSERT, s};
    if (ieq(s, "INTO"))
      return {TokenKind::KW_INTO, s};
    if (ieq(s, "VALUES"))
      return {TokenKind::KW_VALUES, s};
    if (ieq(s, "SELECT"))
      return {TokenKind::KW_SELECT, s};
    if (ieq(s, "FROM"))
      return {TokenKind::KW_FROM, s};
    if (ieq(s, "WHERE"))
      return {TokenKind::KW_WHERE, s};
    if (ieq(s, "PRIMARY"))
      return {TokenKind::KW_PRIMARY, s};
    if (ieq(s, "KEY"))
      return {TokenKind::KW_KEY, s};
    if (ieq(s, "INT"))
      return {TokenKind::KW_INT, s};
    if (ieq(s, "TEXT"))
      return {TokenKind::KW_TEXT, s};
    if (ieq(s, "BEGIN"))
      return {TokenKind::KW_BEGIN, s};
    if (ieq(s, "COMMIT"))
      return {TokenKind::KW_COMMIT, s};
    if (ieq(s, "ROLLBACK"))
      return {TokenKind::KW_ROLLBACK, s};
    if (ieq(s, "EXPLAIN"))
      return {TokenKind::KW_EXPLAIN, s};

    return Token{TokenKind::Identifier, s};
  }

  Token Lexer::next()
  {
    skip_ws();
    char c = peek();
    if (c == '\0')
      return {TokenKind::End, ""};

    if (c == '(')
    {
      get();
      return {TokenKind::LParen, "("};
    }
    if (c == ')')
    {
      get();
      return {TokenKind::RParen, ")"};
    }
    if (c == ',')
    {
      get();
      return {TokenKind::Comma, ","};
    }
    if (c == ';')
    {
      get();
      return {TokenKind::Semicolon, ";"};
    }
    if (c == '*')
    {
      get();
      return {TokenKind::Star, "*"};
    }
    if (c == '=')
    {
      get();
      return {TokenKind::Equal, "="};
    }

    if (isalpha(static_cast<unsigned char>(c)) || c == '_')
      return lex_identifier_or_keyword();
    if (isdigit(static_cast<unsigned char>(c)))
      return lex_number();
    if (c == '\'')
    {
      get();
      return lex_string();
    }

    throw runtime_error(string("unexpected character: '") + c + "'");
  }

} // namespace minisql::sql