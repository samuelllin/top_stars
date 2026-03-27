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
    """SQLite 数据库连接上下文管理器，自动提交并关闭连接"""
    con = sqlite3.connect('stars.db')
    cur = con.cursor()
    try:
        yield cur
    finally:
        con.commit()
        con.close()


def json_search(page=1, language=None):
    """调用 GitHub Search API，按 stars 数量降序搜索仓库，支持按语言过滤"""
    l = f"&language:{language}" if language else ''
    url = f'https://api.github.com/search/repositories?q=stars:>1000{l}&per_page=100&page={page}'
    f = urllib.request.urlopen(url)
    return json.load(f)


def init_database():
    """初始化数据库：创建 repositories 和 stats 表，并兼容性追加新字段"""
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
        db.execute(
            "CREATE TABLE IF NOT EXISTS stats "
            "(day date, repository_id integer, stars integer, forks integer, "
            "PRIMARY KEY ( day, repository_id))"
        )

        # 兼容已有数据库，追加新字段（若字段已存在则忽略报错）
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


def fetch_and_store_repos():
    """从 GitHub API 抓取前 10 页热门仓库数据，并写入数据库"""
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
                db.execute(
                    "INSERT OR REPLACE INTO stats VALUES (DATE('now'),?,?,?)",
                    (result['id'], result['stargazers_count'], result['forks'])
                )
            print(f"Processed page {page}")
            time.sleep(3)


def write_table_md():
    """将最新一天 stars 排行写入 table.md（Markdown 格式，简洁列）"""
    with database() as db:
        latest_day = db.execute(
            "SELECT MAX(day) FROM stats"
        ).fetchone()[0]
        with open('table.md', 'w', encoding='utf-8') as f:
            result = db.execute(
                "SELECT stars, forks, language, full_name, description, html_url "
                "FROM stats JOIN repositories ON stats.repository_id = repositories.id "
                "WHERE stats.day = ? ORDER BY stats.stars DESC",
                (latest_day,)
            )
            c = 1
            f.write(
                '\n'
                '| Number | Stars | Forks | Language| Name  | Description |\n'
                '| :---   | :---: | :---: | :---    | :---  | :---        |\n'
            )
            for row in result:
                stars, forks, language, full_name, description, html_url = row
                stars, forks, language = str(stars), str(forks), str(language)
                description = html.escape((description or '').replace('|', '')[:500])
                name_link = f"[{full_name}]({html_url})" if html_url else full_name
                line = f"| {c} | {stars} | {forks} | {language} | {name_link} | {description} | \n"
                f.write(line)
                c += 1


def write_full_info_table():
    """将最新一天 stars 排行写入 table_full_info.md（包含 URL、大小、时间等完整字段）"""
    with database() as db:
        latest_day = db.execute(
            "SELECT MAX(day) FROM stats"
        ).fetchone()[0]
        with open('table_full_info.md', 'w', encoding='utf-8') as f:
            result = db.execute("""
                SELECT stats.stars, stats.forks, r.language, r.full_name, r.description,
                       r.html_url, r.clone_url, r.size, r.created_at, r.updated_at, r.pushed_at
                FROM stats
                JOIN repositories r ON stats.repository_id = r.id
                WHERE stats.day = ?
                ORDER BY stats.stars DESC
            """, (latest_day,))
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


def write_weekly_stars_json():
    """查询最近 20 个周期数据，计算每周期 stars 增量，输出 weekly_stars.json"""
    with database() as db:
        days_result = db.execute(
            "SELECT DISTINCT day FROM stats ORDER BY day DESC LIMIT 20"
        ).fetchall()
        all_days = [d[0] for d in days_result]

        days_data = []
        for i, day in enumerate(all_days):
            prev_day = all_days[i + 1] if i + 1 < len(all_days) else None

            if prev_day:
                # 与前一天对比，计算 stars 增量
                query = """
                SELECT r.full_name,
                       COALESCE(r.description, ''),
                       COALESCE(r.language, ''),
                       COALESCE(r.html_url, ''),
                       s1.stars,
                       COALESCE(s2.stars, 0) AS prev_stars,
                       (s1.stars - COALESCE(s2.stars, 0)) AS stars_gained
                FROM stats s1
                JOIN repositories r ON s1.repository_id = r.id
                LEFT JOIN stats s2
                    ON s2.repository_id = s1.repository_id AND s2.day = ?
                WHERE s1.day = ?
                ORDER BY stars_gained DESC
                LIMIT 50
                """
                rows = db.execute(query, (prev_day, day)).fetchall()
            else:
                # 最早一天无前一天数据，直接按总 stars 排序
                query = """
                SELECT r.full_name,
                       COALESCE(r.description, ''),
                       COALESCE(r.language, ''),
                       COALESCE(r.html_url, ''),
                       s1.stars,
                       0,
                       s1.stars
                FROM stats s1
                JOIN repositories r ON s1.repository_id = r.id
                WHERE s1.day = ?
                ORDER BY s1.stars DESC
                LIMIT 50
                """
                rows = db.execute(query, (day,)).fetchall()

            repos = []
            for row in rows:
                full_name, description, language, html_url, stars, prev_stars, stars_gained = row
                repos.append({
                    "full_name": full_name,
                    "description": description[:300] if description else "",
                    "language": language or "",
                    "html_url": html_url or "",
                    "stars": stars,
                    "prev_stars": prev_stars,
                    "stars_gained": stars_gained,
                })

            days_data.append({
                "day": day,
                "prev_day": prev_day or "",
                "top_repos": repos,
            })

    with open("weekly_stars.json", "w", encoding="utf-8") as f:
        json.dump({"days": days_data}, f, ensure_ascii=False)


def main():
    """主流程：依次执行数据库初始化、数据抓取、各报告生成"""
    init_database()
    fetch_and_store_repos()
    write_table_md()
    write_full_info_table()
    write_weekly_stars_json()


if __name__ == '__main__':
    main()
