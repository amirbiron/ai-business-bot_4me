"""
Seed Data — מאכלס את מאגר הידע (Knowledge Base) בנתוני העסק.

הרץ סקריפט זה כדי לאתחל את מסד הנתונים עם מידע עסקי.
לעדכון הנתונים יש לשנות את DEMO_ENTRIES ולהריץ מחדש לאחר מחיקת ה-DB.

שימוש: python -m ai_chatbot.seed_data
"""

import logging
from datetime import date

import holidays as holidays_lib

from ai_chatbot import database as db
from ai_chatbot.rag.engine import rebuild_index

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Knowledge Base Entries — אוטומציה בקלות ─────────────────────────────────

DEMO_ENTRIES = [
    # ── Services ──────────────────────────────────────────────────────────
    {
        "category": "Services",
        "title": "דף נחיתה / פורטפוליו",
        "content": """אוטומציה בקלות — שירות: דף נחיתה / פורטפוליו
בניית דף נחיתה או פורטפוליו אישי/עסקי מותאם אישית.
המחיר: 250-500 ש"ח (תלוי בהיקף ומורכבות הפרויקט).
הדף נבנה לפי צרכי הלקוח ויכלול מידע על השירותים, תמונות, ופרטי קשר.
לפרטים נוספים ניתן לפנות בטלפון 0543978620 או במייל amirbiron@gmail.com."""
    },
    {
        "category": "Services",
        "title": "בוט לידים מפייסבוק",
        "content": """אוטומציה בקלות — שירות: בוט לידים מפייסבוק
בוט חכם שסורק קבוצות פייסבוק ושולח לך פוסטים רלוונטיים בזמן אמת.
תכונות עיקריות:
- פאנל ניהול פשוט ומסודר
- הגדרת קבוצות פייסבוק לסריקה
- הגדרת מילות מפתח לחיפוש
- הגדרת מילים חוסמות — פוסטים עם מילים אלו לא יישלחו
- AI שבודק ומסנן את הלידים לפי הנחיה אישית שתגדיר
- סריקה כל שעה (לא ניתן כל 5 דקות — יגרום לחסימה)
- אפשרות "שלח תמיד" — פוסטים עם מילים מסוימות נשלחים ישירות ללא בדיקת AI
- הגדרת שעות שקט — ללא התראות בשעות מסוימות
- עובד גם על קבוצות פרטיות
מגבלות:
- נדרש חשבון פייסבוק לא בשימוש
- מוגבל ל-60 דקות סריקה ביום כדי להימנע מחסימת פייסבוק (עד 10-15 קבוצות)
מחיר:
- שבוע ניסיון חינם
- 300 ש"ח התקנה ראשונית
- 70 ש"ח לחודש (תחזוקה ותמיכה שוטפת)"""
    },
    {
        "category": "Services",
        "title": "צ'אטבוט טלגרם לעסקים קטנים",
        "content": """אוטומציה בקלות — שירות: צ'אטבוט טלגרם לעסקים קטנים
בוט טלגרם חכם לשירות לקוחות אוטומטי עבור עסקים קטנים.
הבוט עונה על שאלות לקוחות בצ'אט, מספק מידע על שירותים ומחירים, ומנתב לנציג אנושי בעת הצורך.
מחיר:
- שבוע ניסיון חינם
- 250 ש"ח התקנה ראשונית
- 60 ש"ח לחודש (תחזוקה ותמיכה שוטפת)"""
    },
    {
        "category": "Services",
        "title": "שירותי אוטומציה בהתאמה אישית",
        "content": """אוטומציה בקלות — שירות: אוטומציה בהתאמה אישית
פיתוח פתרונות אוטומציה מותאמים אישית לצרכים הייחודיים של העסק שלך.
מחיר:
- לרוב עד 1,000 ש"ח חד-פעמי
- 30-60 ש"ח לחודש (תחזוקה שוטפת, בהתאם להיקף)
לפרטים ותיאום: 0543978620 | amirbiron@gmail.com"""
    },

    # ── Pricing ───────────────────────────────────────────────────────────
    {
        "category": "Pricing",
        "title": "מחירון שירותים",
        "content": """אוטומציה בקלות — מחירון שירותים
דף נחיתה / פורטפוליו:
- מחיר: 250-500 ש"ח (חד-פעמי, תלוי בהיקף הפרויקט)
בוט לידים מפייסבוק:
- שבוע ניסיון: חינם
- התקנה ראשונית: 300 ש"ח
- דמי תחזוקה חודשיים: 70 ש"ח לחודש
צ'אטבוט טלגרם לעסקים קטנים:
- שבוע ניסיון: חינם
- התקנה ראשונית: 250 ש"ח
- דמי תחזוקה חודשיים: 60 ש"ח לחודש
שירותי אוטומציה בהתאמה אישית:
- עלות חד-פעמית: לרוב עד 1,000 ש"ח
- דמי תחזוקה חודשיים: 30-60 ש"ח (תלוי בהיקף)
הערות:
- המחירים אינם כוללים מע"מ (אם רלוונטי)
- יש אפשרות להנחה — ניתן לשאול
- זמן הכנה: לרוב עד שבוע-שבועיים לכל פרויקט"""
    },

    # ── Hours ─────────────────────────────────────────────────────────────
    {
        "category": "Hours",
        "title": "זמינות ושעות פעילות",
        "content": """אוטומציה בקלות — זמינות
השירות ניתן לפי תיאום אישי.
לפנייה: 0543978620 | amirbiron@gmail.com
זמן תגובה: לרוב תוך יום עסקים.
הערות לגבי חגים: בחגים ייתכן עיכוב בתגובה — אנא שלחו הודעה ונחזור בהקדם."""
    },

    # ── Location ──────────────────────────────────────────────────────────
    {
        "category": "Location",
        "title": "פרטי קשר",
        "content": """אוטומציה בקלות — פרטי קשר
טלפון: 0543978620
אימייל: amirbiron@gmail.com
פורטפוליו: https://dev-portfolio-clkk.onrender.com/
השירות ניתן מרחוק — אין צורך בפגישה פיזית.
ניתן לפנות בכל שאלה או לתיאום שיחת היכרות."""
    },

    # ── Staff ─────────────────────────────────────────────────────────────
    {
        "category": "Staff",
        "title": "הצוות שלנו",
        "content": """אוטומציה בקלות — הצוות
אמיר בירון — מייסד ומפתח
- מפתח Full Stack עם התמחות באוטומציה, בוטים ופתרונות דיגיטליים לעסקים קטנים
- מנסיון רב בבניית בוטים לטלגרם, כלי לידים, ודפי נחיתה
- נגיש ופשוט לעבוד איתו — מסביר הכל בעברית פשוטה
- ימי עבודה: ימי חול, לפי תיאום"""
    },

    # ── Policies ──────────────────────────────────────────────────────────
    {
        "category": "Policies",
        "title": "מדיניות הזמנות ותשלומים",
        "content": """אוטומציה בקלות — מדיניות הזמנות
תהליך הזמנה:
- פנייה ראשונית בטלפון או מייל לתיאום הצרכים
- קבלת הצעת מחיר מותאמת
- לאחר אישור — תחילת עבודה
זמן הכנה:
- לרוב עד שבוע-שבועיים לכל פרויקט
ניסיון חינם:
- בוט לידים מפייסבוק וצ'אטבוט טלגרם — שבוע ניסיון חינם לפני תשלום
הנחות:
- יש אפשרות לקבל הנחה — כדאי לשאול
תשלום:
- לפי תיאום — מזומן, העברה בנקאית, ביט וכו'"""
    },
    {
        "category": "Policies",
        "title": "מדיניות תמיכה ושירות",
        "content": """אוטומציה בקלות — תמיכה ושירות
תמיכה שוטפת:
- כלולה בדמי התחזוקה החודשיים
- זמינות לפניות בטלפון ובמייל
- תגובה לרוב תוך יום עסקים
עדכונים ושינויים:
- שינויים קטנים — ניתן לבקש ויטופלו בהקדם
- שינויים גדולים — תלוי בהיקף, ייתכן תוספת עלות
פרטיות:
- מידע הלקוח נשמר בסודיות ולא מועבר לצד שלישי"""
    },

    # ── FAQ ───────────────────────────────────────────────────────────────
    {
        "category": "FAQ",
        "title": "שאלות נפוצות (FAQ)",
        "content": """שאלות נפוצות — אוטומציה בקלות
ש: אפשר פרטים על בוט הלידים מפייסבוק?
ת: בקצרה — זה בוט שסורק קבוצות פייסבוק ושולח לך פוסטים רלוונטיים.
יש פאנל פשוט לתפעול:
- מגדיר קבוצות פייסבוק לסריקה
- מגדיר מילות מפתח רלוונטיות
- מגדיר מילים חוסמות — פוסטים עם מילים כאלה לא נשלחים
- ה-AI בודק את הפוסטים לפי הנחיה שתגדיר — מה שסומן כליד נשלח אליך מיד
- הבוט סורק כל שעה (לא ניתן כל 5 דקות — יגרום לחסימה)
- ניתן להגדיר מילים "שלח תמיד" — נשלחות ישירות ללא בדיקת AI
- ניתן להגדיר שעות שקט — ללא התראות
- עובד גם על קבוצות פרטיות
מגבלות: נדרש חשבון פייסבוק לא בשימוש. מוגבל ל-60 דקות סריקה ביום (עד 10-15 קבוצות).
ש: האם יש אפשרות להנחה?
ת: כן, יש אפשרות כזאת — כדאי לשאול ישירות.
ש: תוך כמה זמן ההזמנות מוכנות?
ת: לרוב עד שבוע-שבועיים לכל פרויקט.
ש: אפשר דוגמה לפורטפוליו?
ת: כן! זה הפורטפוליו האישי: https://dev-portfolio-clkk.onrender.com/
ש: איך מתחילים?
ת: פשוט צרו קשר בטלפון 0543978620 או במייל amirbiron@gmail.com ונתאם שיחת היכרות קצרה.
ש: האם יש ניסיון חינם?
ת: כן! בוט הלידים מפייסבוק וצ'אטבוט הטלגרם מגיעים עם שבוע ניסיון חינם."""
    },

    # ── Promotions ────────────────────────────────────────────────────────
    {
        "category": "Promotions",
        "title": "מבצעים והטבות",
        "content": """מבצעים והטבות — אוטומציה בקלות
שבוע ניסיון חינם:
- בוט לידים מפייסבוק: שבוע ניסיון חינם לפני תשלום
- צ'אטבוט טלגרם: שבוע ניסיון חינם לפני תשלום
הנחות:
- יש אפשרות להנחה — כדאי לשאול ישירות
מחירים נגישים:
- התקנה ראשונית מ-250 ש"ח בלבד
- תחזוקה חודשית מ-60 ש"ח בלבד"""
    },
]


def _seed_business_hours():
    """Seed default business hours and upcoming Israeli holidays as special days."""
    db.seed_default_business_hours()
    logger.info("Seeded default business hours.")

    # Seed Israeli holidays for the current and next year as special days
    current_year = date.today().year
    years = [current_year, current_year + 1]
    existing = {sd["date"] for sd in db.get_all_special_days()}

    count = 0
    for year in years:
        il_holidays = holidays_lib.Israel(years=year, language="he")
        for hol_date, hol_name in sorted(il_holidays.items()):
            date_str = hol_date.strftime("%Y-%m-%d")
            if date_str not in existing:
                db.add_special_day(
                    date_str=date_str,
                    name=hol_name,
                    is_closed=True,
                )
                existing.add(date_str)
                count += 1

    if count:
        logger.info("Seeded %d Israeli holidays as special days.", count)
    else:
        logger.info("Israeli holidays already seeded.")


def seed_database():
    """Populate the database with demo data."""
    logger.info("Initializing database...")
    db.init_db()

    # Always seed business hours & holidays (idempotent)
    _seed_business_hours()

    # Check if data already exists (include inactive entries to avoid duplicates)
    existing = db.get_all_kb_entries(active_only=False)
    if existing:
        logger.info("Database already has %s entries. Skipping seed.", len(existing))
        logger.info("To re-seed, delete the database file first: ai_chatbot/data/chatbot.db")
        return False

    logger.info("Seeding %s knowledge base entries...", len(DEMO_ENTRIES))

    for entry in DEMO_ENTRIES:
        entry_id = db.add_kb_entry(
            category=entry["category"],
            title=entry["title"],
            content=entry["content"],
        )
        logger.info(
            "  Added: [%s] %s (ID: %s)",
            entry["category"],
            entry["title"],
            entry_id,
        )

    logger.info("Seed data inserted successfully!")
    return True


def seed_and_index():
    """Seed the database and build the RAG index."""
    was_seeded = seed_database()

    if was_seeded:
        logger.info("Building RAG index...")
        rebuild_index()
        logger.info("RAG index built successfully!")
    else:
        logger.info("Checking if RAG index needs rebuilding...")
        from ai_chatbot.rag.vector_store import get_vector_store

        store = get_vector_store()
        if store.index is None or store.index.ntotal == 0:
            logger.info("Index is empty. Rebuilding...")
            rebuild_index()
            logger.info("RAG index built successfully!")
        else:
            logger.info("RAG index already exists.")


if __name__ == "__main__":
    seed_and_index()
