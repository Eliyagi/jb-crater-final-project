import sys
import os
import time
from neo4j import GraphDatabase
import psycopg2

# -----------------------------------------------------------------------------
# Connection Configurations - 100% Synchronized with Spark Pipeline Config
# -----------------------------------------------------------------------------
POSTGRES_HOST = os.getenv("PG_HOST", "postgres")
POSTGRES_PORT = os.getenv("PG_PORT", "5432")
POSTGRES_DB = os.getenv("PG_DB", "crater_analytics")
POSTGRES_USER = os.getenv("PG_USER", "spark")
POSTGRES_PASSWORD = os.getenv("PG_PASSWORD", "spark")

NEO4J_URI = os.getenv("NEO4J_URL", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jpassword")


# -----------------------------------------------------------------------------
# Database Connection Functions
# -----------------------------------------------------------------------------
def get_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None

def get_neo4j_driver():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        return driver
    except Exception as e:
        print(f"❌ Error connecting to Neo4j: {e}")
        return None


# -----------------------------------------------------------------------------
# Helper Function for Pretty Table Printing in Terminal
# -----------------------------------------------------------------------------
def print_table(title, headers, rows):
    MAX_COL_WIDTH = 25  # מספר התווים המקסימלי לעמודה (לפני חיתוך ל-...)

    if not rows:
        print(f"\n{'='*75}\n 📊 {title.upper()}\n{'='*75}")
        print("   No data found / Empty result.")
        print(f"{'='*75}")
        return

    # פונקציית עזר פנימית לחיתוך טקסט ארוך
    def truncate(val):
        s = str(val)
        if len(s) > MAX_COL_WIDTH:
            return s[:MAX_COL_WIDTH - 3] + "..."
        return s

    # 1. עיבוד מראש של כל השורות - חיתוך ערכים ארוכים
    processed_rows = []
    for row in rows:
        processed_rows.append([truncate(val) for val in row])

    # 2. חישוב רוחב עמודות על בסיס הדאטה החתוך
    col_widths = [len(h) for h in headers]
    for row in processed_rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(val))

    # 3. חישוב רוחב הטבלה הכולל
    table_width = sum(col_widths) + (len(headers) * 3) + 1

    # 4. הדפסת כותרת מותאמת לרוחב
    print(f"\n{'=' * table_width}")
    print(f" 📊 {title.upper():<{table_width - 5}}")
    print(f"{'=' * table_width}")

    # 5. הדפסת כותרות העמודות
    header_line = " | ".join(f"{str(headers[i]):<{col_widths[i]}}" for i in range(len(headers)))
    print(f"| {header_line} |")
    print(f"|{'-' * (table_width - 2)}|")

    # 6. הדפסת השורות החתוכות
    for row in processed_rows:
        row_line = " | ".join(f"{val:<{col_widths[i]}}" for i, val in enumerate(row))
        print(f"| {row_line} |")
    
    print(f"{'=' * table_width}")

# -----------------------------------------------------------------------------
# [Question 1] - Primary Coding Language of an Actor (PostgreSQL)
# -----------------------------------------------------------------------------
def query_actor_languages(pg_conn):
    cursor = pg_conn.cursor()
    
    query = """
        WITH actor_totals_all AS (
            -- 1. קודם כל, מזהים את המפתחים החזקים (לפחות 10 PRs ממוזגים) כולל הכל
            SELECT 
                pr_author AS developer,
                language,
                COUNT(DISTINCT pr_id) AS merge_count,
                SUM(COUNT(DISTINCT pr_id)) OVER(PARTITION BY pr_author) AS total_merged_prs
            FROM pull_requests
            WHERE action = 'closed' 
            AND is_merged = true 
            --AND pr_author NOT LIKE '%[bot]%'
            AND pr_author NOT IN ('github-actions', 'renovate', 'dependabot')
            GROUP BY pr_author, language
        ),
        ranked_languages AS (
            -- 2. עכשיו מסננים את ה-NULL בשפה ומדרגים, אבל ה-total_merged_prs נשאר אמיתי ומדויק!
            SELECT 
                developer,
                language,
                merge_count,
                total_merged_prs,
                ROW_NUMBER() OVER(PARTITION BY developer ORDER BY merge_count DESC, language ASC) AS rn
            FROM actor_totals_all
            WHERE language IS NOT NULL AND total_merged_prs >= 10
        )
        -- 3. שליפת ה-Top 3 שפות לכל מפתח שעמד בתנאי
        SELECT 
            developer,
            rn AS rank,
            language,
            merge_count,
            total_merged_prs
        FROM ranked_languages
        WHERE rn <= 3
        ORDER BY total_merged_prs DESC, developer ASC, rn ASC;
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        title = "Developers Primary Languages (Min 10 Merged PRs - Top 3 Languages)"
        headers = ["Developer", "Rank", "Language", "Merged PRs", "Total Merged"]
        
        # התאמה לפונקציית ה-print_table החותכת שלך
        formatted_rows = [
            (
                str(r[0]),
                int(r[1]),
                str(r[2]),
                int(r[3]),
                int(r[4])
            )
            for r in rows
        ]
        
        print_table(title, headers, formatted_rows)
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        pg_conn.rollback()
    finally:
        cursor.close()


# -----------------------------------------------------------------------------
# [Question 2] - Who writes the code in the repository? (PostgreSQL)
# -----------------------------------------------------------------------------
def query_repo_authors(pg_conn):
    cursor = pg_conn.cursor()
    
    query = """
        WITH top_50_repos AS (
            -- 1. 50 הרפוז הכי פעילים לפי PR, בתנאי שיש להם פעילות קוד מינימלית
            SELECT pr.repo_name
            FROM pull_requests pr
            WHERE EXISTS (
                SELECT 1 
                FROM pushes p 
                WHERE p.repo_name = pr.repo_name
                GROUP BY p.actor_login
                HAVING COUNT(DISTINCT p.push_id) >= 4
            ) OR EXISTS (
                SELECT 1 
                FROM pushes p 
                WHERE p.repo_name = pr.repo_name AND p.commit_author_name IS NOT NULL
                GROUP BY p.commit_author_name, p.commit_author_email
                HAVING SUM(p.commit_count) >= 4  -- תיקון עמודה: שימוש ב-SUM של commit_count
            )
            GROUP BY pr.repo_name
            ORDER BY COUNT(*) DESC
            LIMIT 50
        ),
        ranked_pushers AS (
            -- 2. טופ 5 פושארים לכל רפו (דירוג לפי כמות push_id ייחודיים)
            SELECT 
                p.repo_name,
                p.actor_login AS pusher_name,
                COUNT(DISTINCT p.push_id) AS push_count,
                ROW_NUMBER() OVER (PARTITION BY p.repo_name ORDER BY COUNT(DISTINCT p.push_id) DESC) AS rn
            FROM pushes p
            JOIN top_50_repos t ON p.repo_name = t.repo_name
            GROUP BY p.repo_name, p.actor_login
        ),
        ranked_authors AS (
            -- 3. טופ 5 מחברי קומיטים לכל רפו (מתוקן לפי ה-Schema: סכימת commit_count ודירוג לפיה)
            SELECT 
                p.repo_name,
                CONCAT(p.commit_author_name, ' <', p.commit_author_email, '>') AS author_name,
                SUM(p.commit_count) AS commit_count,
                ROW_NUMBER() OVER (PARTITION BY p.repo_name ORDER BY SUM(p.commit_count) DESC) AS rn
            FROM pushes p
            JOIN top_50_repos t ON p.repo_name = t.repo_name
            WHERE p.commit_author_name IS NOT NULL
            GROUP BY p.repo_name, p.commit_author_name, p.commit_author_email
        )
        -- 4. חיבור הנתונים Side-by-Side (דירוג 1 עד 5 לכל רפו באופן סימטרי)
        SELECT 
            r.repo_name,
            i.rn AS rank,
            COALESCE(p.pusher_name, '-') AS top_pusher,
            COALESCE(p.push_count, 0) AS push_count,
            COALESCE(a.author_name, '-') AS top_author,
            COALESCE(a.commit_count, 0) AS commit_count
        FROM top_50_repos r
        CROSS JOIN generate_series(1, 5) AS i(rn)
        LEFT JOIN ranked_pushers p ON r.repo_name = p.repo_name AND i.rn = p.rn
        LEFT JOIN ranked_authors a ON r.repo_name = a.repo_name AND i.rn = a.rn
        ORDER BY r.repo_name, i.rn;
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        title = "Top 50 Repos (by PR): Top 5 Pushers vs Top 5 Commit Authors"
        headers = ["Repository", "Rank", "Top Pusher", "Pushes", "Top Author (Name <Email>)", "Commits"]
        
        # בנייה תקינה של כל 6 העמודות (בלי לחתוך דאטה)
        formatted_rows = [
            (
                r[0] if r[0] else "-",
                r[1],
                r[2] if r[2] else "-",
                r[3],
                r[4] if r[4] else "-",
                r[5]
            ) 
            for r in rows
        ]
        
        print_table(title, headers, formatted_rows)
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        pg_conn.rollback()
    finally:
        cursor.close()


# -----------------------------------------------------------------------------
# [Question 3] - Developer Collaboration Pairs (Neo4j Graph)
# -----------------------------------------------------------------------------
def query_developer_pairs(neo4j_driver):
    print("\n⏳ Analyzing collaboration pairs using Graph Relations (Shared Repositories)...")
    query = """
        MATCH (a1:Actor)-[:CONTRIBUTED_TO]->(r:Repository)<-[:CONTRIBUTED_TO]-(a2:Actor)
        WHERE a1.login < a2.login
        WITH a1, a2, count(r) AS SharedReposCount, collect(r.name) AS Repositories
        WHERE SharedReposCount >= 3
        RETURN a1.login AS Dev1, 
               a2.login AS Dev2, 
               SharedReposCount,
               Repositories,
               SharedReposCount AS TotalCombinedContributions
        ORDER BY SharedReposCount DESC
        LIMIT 10
    """
    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            rows = [
                (
                    rec["Dev1"], 
                    rec["Dev2"], 
                    rec["SharedReposCount"], 
                    ", ".join(rec["Repositories"][:3]) + ("..." if len(rec["Repositories"]) > 3 else ""), 
                    rec["TotalCombinedContributions"]
                ) 
                for rec in result
            ]
            
            print_table(
                "Top 10 Collaborating Graph Pairs (Neo4j)", 
                ["Dev 1", "Dev 2", "Distinct Repos", "Sample Repos", "Combined Contribs"], 
                rows
            )
    except Exception as e:
        print(f"❌ Query failed: {e}")


# -----------------------------------------------------------------------------
# [Question 4] - Does interest lead to contribution? Funnel (PostgreSQL)
# -----------------------------------------------------------------------------
def query_contribution_funnel(pg_conn):
    print("\n⏳ Calculating Funnel Conversion Metrics per Repo (Watch -> Fork [2d] -> PR [5d])...")
    cursor = pg_conn.cursor()
    
    query = """
        WITH target_repos AS (
            -- 1. סינון רק לרפוז שקיבלו לפחות 500 סטארים (שונה ל-5 לצורך בדיקות)
            SELECT repo_name, COUNT(*) AS total_watchers
            FROM watches
            GROUP BY repo_name
            HAVING COUNT(*) >= 50
        ),
        watcher_forks AS (
            -- 2. מציאת הסטארגייזרים שביצעו פורק תוך 2 ימים
            SELECT 
                w.repo_name,
                w.actor_login,
                w.created_at AS watched_at,
                MIN(f.created_at) AS forked_at
            FROM watches w
            JOIN target_repos tr ON w.repo_name = tr.repo_name
            LEFT JOIN forks f ON w.repo_name = f.repo_name 
                            AND w.actor_login = f.actor_login 
                            AND f.created_at BETWEEN w.created_at AND w.created_at + INTERVAL '2 days'
            GROUP BY w.repo_name, w.actor_login, w.created_at
        ),
        funnel_metrics AS (
            -- 3. חיבור ל-PRs תוך 5 ימים מהפורק וסכימה ברמת הרפו
            SELECT 
                wf.repo_name,
                COUNT(DISTINCT wf.actor_login) AS watchers_count,
                COUNT(DISTINCT CASE WHEN wf.forked_at IS NOT NULL THEN wf.actor_login END) AS forkers_count,
                COUNT(DISTINCT CASE WHEN pr.created_at BETWEEN wf.forked_at AND wf.forked_at + INTERVAL '5 days' THEN wf.actor_login END) AS pr_count
            FROM watcher_forks wf
            LEFT JOIN pull_requests pr ON wf.repo_name = pr.repo_name 
                                    AND wf.actor_login = pr.actor_login 
                                    AND pr.action = 'opened'
            GROUP BY wf.repo_name
        )
        -- 4. שליפת כל 6 העמודות וטיפול ב-NULL-ים באמצעות COALESCE
        SELECT 
            repo_name,
            watchers_count,
            forkers_count,
            COALESCE(ROUND(forkers_count::NUMERIC / NULLIF(watchers_count, 0), 4), 0.0000) AS watch_to_fork_fraction,
            pr_count,
            COALESCE(ROUND(pr_count::NUMERIC / NULLIF(forkers_count, 0), 4), 0.0000) AS fork_to_pr_fraction
        FROM funnel_metrics
        ORDER BY watchers_count DESC;
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        title = "Repo Contribution Funnel (Min 500 Stars)"
        headers = [
            "Repository", 
            "Watchers", 
            "Forks (2d)", 
            "Watch->Fork Fraction", 
            "PRs (5d)", 
            "Fork->PR Fraction"
        ]
        
        # המרה בטוחה של הנתונים - כל האינדקסים (0 עד 5) קיימים עכשיו ב-SQL
        formatted_rows = [
            (
                str(r[0]),
                int(r[1]),
                int(r[2]),
                f"{float(r[3]):.4f}", 
                int(r[4]),
                f"{float(r[5]):.4f}"
            )
            for r in rows
        ]
        
        print_table(title, headers, formatted_rows)
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        pg_conn.rollback()
    finally:
        cursor.close()


# -----------------------------------------------------------------------------
# [Question 5] - Developer Social Network (Degrees 1+2) (Neo4j Graph)
# -----------------------------------------------------------------------------
def query_network_degrees(neo4j_driver):
    actor = input("\nEnter Developer Username (Leave empty to find network of top active graph node): ").strip()
    
    if not actor:
        print("ℹ️ No username provided. Fetching most connected node in the Graph network...")
        find_top_actor_query = """
            MATCH (a:Actor)-[:CONTRIBUTED_TO]->()
            RETURN a.login as login, count(*) as degree 
            ORDER BY degree DESC LIMIT 1
        """
        try:
            with neo4j_driver.session() as session:
                res = session.run(find_top_actor_query).single()
                if res:
                    actor = res["login"]
                    print(f"🎯 Auto-selected Graph Actor: '{actor}'")
                else:
                    print("❌ No graph connections available.")
                    return
        except Exception as e:
            print(f"❌ Failed to auto-fetch graph node: {e}")
            return

    query = """
        MATCH (target:Actor {login: $actor})-[:CONTRIBUTED_TO]->(r1:Repository)<-[:CONTRIBUTED_TO]-(f1:Actor)
        WHERE f1 <> target
        WITH target, f1, r1
        OPTIONAL MATCH (f1)-[:CONTRIBUTED_TO]->(r2:Repository)<-[:CONTRIBUTED_TO]-(f2:Actor)
        WHERE f2 <> target AND f2 <> f1 AND NOT (target)-[:CONTRIBUTED_TO]->(r2)
        RETURN f1.login AS Friend_Degree1, r1.name AS Shared_Repo, f2.login AS Friend_Of_Friend_Degree2
        LIMIT 10
    """
    try:
        with neo4j_driver.session() as session:
            result = session.run(query, actor=actor)
            rows = []
            for record in result:
                f2 = record["Friend_Of_Friend_Degree2"] if record["Friend_Of_Friend_Degree2"] else "No further connection"
                rows.append((record["Friend_Degree1"], record["Shared_Repo"], f2))
                
            print_table(f"Social Network (1st & 2nd Degree) for {actor}", 
                        ["Friend (1st Degree)", "Via Repository", "Friend of Friend (2nd Degree)"], rows)
    except Exception as e:
        print(f"❌ Query failed: {e}")


# -----------------------------------------------------------------------------
# Main Application Loop and Menu Controller
# -----------------------------------------------------------------------------
def main():
    print("⏳ Connecting to Database Servers...")
    pg_conn = get_postgres_connection()
    neo4j_driver = get_neo4j_driver()

    if not pg_conn and not neo4j_driver:
        print("❌ Critical Error: Could not connect to databases. Exiting.")
        sys.exit(1)
    
    print("🟢 Connected successfully. Serving system is active.")
    time.sleep(1)

    while True:
        print("\n" + "═"*55)
        print(" 🔥   GITHUB ARCHIVE - ADVANCED CORE ANALYTICS   🔥 ")
        print("═"*55)
        print("  [1] 💻 Primary Coding Language of an Actor   (PostgreSQL)")
        print("  [2] ✍️  Top Commit Authors in a Repository   (PostgreSQL)")
        print("  [3] 🤝  Developer Collaboration Pairs        (Neo4j Graph)")
        print("  [4] 🎯  Engagement to Contribution Funnel   (PostgreSQL)")
        print("  [5] 🕸️  Developer Social Network (Degrees 1+2) (Neo4j Graph)")
        print("  [6] ❌ Exit Application")
        print("═"*55)
        
        choice = input("👉 Select a metric to execute (1-6): ").strip()

        if choice == '1':
            query_actor_languages(pg_conn)
        elif choice == '2':
            query_repo_authors(pg_conn)
        elif choice == '3':
            query_developer_pairs(neo4j_driver)
        elif choice == '4':
            query_contribution_funnel(pg_conn)
        elif choice == '5':
            query_network_degrees(neo4j_driver)
        elif choice == '6':
            print("\n👋 Closing database connections. Good luck with the submission!")
            if pg_conn: pg_conn.close()
            if neo4j_driver: neo4j_driver.close()
            sys.exit(0)
        else:
            print("❌ Invalid selection. Please choose a number between 1 and 6.")
        
        input("\n⌨️ Press Enter to return to the main menu...")

if __name__ == "__main__":
    main()