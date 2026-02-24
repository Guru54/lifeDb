#include <iostream>
#include <string>

#include "sql/parser.h"
#include "engine/executor.h"

using namespace std;

static void print_prompt() { cout << "minisql> " << flush; }

int main()
{
    // data/ folder mein sab kuch store hoga
    minisql::engine::Executor executor("data");

    cout << "MiniSQL (milestone-2)  |  type .exit to quit, .tables to list tables\n";

    string line;
    while (true)
    {
        print_prompt();
        if (!getline(cin, line))
            break;

        if (line == ".exit")
            break;

        if (line == ".tables")
        {
            if (executor.catalog.tables.empty())
                cout << "(no tables)\n";
            else
                for (auto &[name, _] : executor.catalog.tables)
                    cout << name << "\n";
            continue;
        }

        if (line.empty())
            continue;

        try
        {
            minisql::sql::Parser parser(line);
            auto stmt = parser.parseStatement();
            executor.execute(stmt);
        }
        catch (const exception &ex)
        {
            cout << "ERROR: " << ex.what() << "\n";
        }
    }

    return 0;
}