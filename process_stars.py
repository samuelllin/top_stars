import json
import html
import sqlite3
import urllib.request
import time
from contextlib import contextmanager
from datetime import datetime, timezone


def parse_date(dt_str):
    """将 ISO 8601 字符串（如 '2026-03-25T08:37:36Z'）转换为 date 字符串 '2026-03-25'"""
    if not dt_str:
        return None
    return datetime.fromisoformat(dt_str.replace('Z', '+00:00')).date().isoformat()

@contextmanager
def database():
    con = sqlite3.connect('stars.db')
    cur = con.cursor()
    try:
        yield cur
    finally:
        con.commit()
        con.close()

def json_search(page=1, language=None):
    l = f"&language:{language}" if language else ''
    url = f'https://api.github.com/search/repositories?q=stars:>1000{l}&per_page=100&page={page}'
    f = urllib.request.urlopen(url)
    return json.load(f)

with database() as db:
    db.execute("""CREATE TABLE IF NOT EXISTS repositories (
        id integer primary key,
        full_name text,
        description text,
        language text,
        html_url text,
        clone_url text,
        size integer,
        created_at date,
        updated_at date,
        pushed_at date
    )""")
    db.execute("CREATE TABLE IF NOT EXISTS stats (day date, repository_id integer, stars integer, forks integer, PRIMARY KEY ( day, repository_id))")

    # 兼容已有数据库，追加新字段（若已存在则忽略）
    new_columns = [
        ("html_url", "text"),
        ("clone_url", "text"),
        ("size", "integer"),
        ("created_at", "date"),
        ("updated_at", "date"),
        ("pushed_at", "date"),
    ]
    for col_name, col_type in new_columns:
        try:
            db.execute(f"ALTER TABLE repositories ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass


with database() as db:
    for page in range(1, 11):
        search_results = json_search(page)
        for result in search_results['items']:
            insert_query = """INSERT OR REPLACE INTO repositories
                (id, full_name, description, language, html_url, clone_url, size, created_at, updated_at, pushed_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)"""
            db.execute(insert_query, (
                result['id'],
                result['full_name'],
                result['description'],
                result['language'],
                result['html_url'],
                result['clone_url'],
                result['size'],
                parse_date(result['created_at']),
                parse_date(result['updated_at']),
                parse_date(result['pushed_at']),
            ))
        for result in search_results['items']:
            db.execute("INSERT OR REPLACE INTO stats VALUES (DATE('now'),?,?,?)", (result['id'], result['stargazers_count'], result['forks']))
        print(f"Processed page {page}")
        time.sleep(3)


with database() as db:
    with open('table.md', 'w', encoding='utf-8') as f:
        result = db.execute("SELECT stars, forks, language, full_name, description FROM stats JOIN repositories ON stats.repository_id = repositories.id WHERE stats.day = DATE('now') ORDER BY stats.stars DESC")
        c = 1
        f.write('''
| Number | Stars | Forks | Language| Name  | Description |
| :---   | :---: | :---: | :---    | :---  | :---        |
''')
        for row in result:
            stars, forks, language, full_name, description = (map(str, row))
            description = html.escape(description.replace('|', '')[:500])
            line = f"| {c} | {stars} | {forks} | {language} | {full_name} | {description} | \n"
            f.write(line)
            c += 1


def write_full_info_table():
    with database() as db:
        with open('table_full_info.md', 'w', encoding='utf-8') as f:
            result = db.execute("""
                SELECT stats.stars, stats.forks, r.language, r.full_name, r.description,
                       r.html_url, r.clone_url, r.size, r.created_at, r.updated_at, r.pushed_at
                FROM stats
                JOIN repositories r ON stats.repository_id = r.id
                WHERE stats.day = DATE('now')
                ORDER BY stats.stars DESC
            """)
            c = 1
            f.write(
                '| Number | Stars | Forks | Language | Name | Description'
                ' | Clone URL | Size (KB) | Created At | Updated At | Pushed At |\n'
                '| :---   | :---: | :---: | :---     | :--- | :---'
                ' | :---      | :---:     | :---       | :---       | :---      |\n'
            )
            for row in result:
                stars, forks, language, full_name, description, html_url, clone_url, size, created_at, updated_at, pushed_at = row
                language = language or ''
                description = html.escape((description or '').replace('|', '')[:500])
                name_link = f"[{full_name}]({html_url})" if html_url else full_name
                clone_url = clone_url or ''
                size = str(size) if size is not None else ''
                created_at = created_at or ''
                updated_at = updated_at or ''
                pushed_at = pushed_at or ''
                line = (
                    f"| {c} | {stars} | {forks} | {language} | {name_link} | {description}"
                    f" | {clone_url} | {size} | {created_at} | {updated_at} | {pushed_at} |\n"
                )
                f.write(line)
                c += 1


write_full_info_table()

