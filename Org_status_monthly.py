import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

MONGO_URI = "mongodb+srv://yiscarose:Z754IlLTUkiqfpIZ@by-staging.dxg0wdb.mongodb.net/"
SOURCE_DB_NAME = "htd-core-ms"
REPORT_DB_NAME = "by-reporting"
ORG_STATUS_COLLECTION = "org_status_monthly"

client = AsyncIOMotorClient(MONGO_URI)
source_db = client[SOURCE_DB_NAME]
report_db = client[REPORT_DB_NAME]

async def create_org_status_monthly():
    print("start create_org_status_monthly")
    now_il = datetime.now(ZoneInfo("Asia/Jerusalem"))
    date_str = now_il.strftime("%Y-%m-%d %H:%M")

    before_count = await report_db[ORG_STATUS_COLLECTION].count_documents({"year_month": date_str})

    pipeline = [
        {"$project": {
            "_id": 0,
            "org_id": "$id",
            "org_name": "$name",
            "year_month": {"$literal": date_str},
            "org_created_at": "$created_at",
            "current_plan": 1,
            "language": 1,
            "status": 1
        }},
        {"$addFields": {
            "status": {"$ifNull": ["$status", "active"]}
        }},
        {"$merge": {
            "into": {"db": REPORT_DB_NAME, "coll": ORG_STATUS_COLLECTION},
            "on": ["org_id", "year_month"],
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }}
    ]
    await source_db.command({"aggregate": "organizations", "pipeline": pipeline, "cursor": {}})
    end_time = datetime.now(timezone.utc)
    print("time for create_org_status_monthly:", end_time-now_il)
    print("Completed create_org_status_monthly!")
    after_count = await report_db[ORG_STATUS_COLLECTION].count_documents({"year_month": date_str})
    added = after_count - before_count
    print(f"added {added} in {date_str}")

    return date_str

async def enrich_with_workspaces(minute_str):
    print("\nstart enrich_with_workspaces")

    now_il = datetime.now(ZoneInfo("Asia/Jerusalem"))
    pipeline = [
        {"$group": {
            "_id": "$org_id",
            "ws": {"$sum": 1},
            "ws_learners": {
                "$sum": {
                    "$size": {
                        "$filter": {
                            "input": "$members",
                            "as": "member",
                            "cond": {"$eq": ["$$member.role", 3]}
                        }
                    }
                }
            },
            "ws_users": {
                "$sum": {
                    "$size": {
                        "$filter": {
                            "input": "$members",
                            "as": "member",
                            "cond": {"$in": ["$$member.role", [1, 2, 3]]}
                        }
                    }
                }
            },
            "ws_editors": {
                "$sum": {
                    "$size": {
                        "$filter": {
                            "input": "$members",
                            "as": "member",
                            "cond": {"$in": ["$$member.role", [1, 2]]}
                        }
                    }
                }
            }



        }},
        {"$project": {
            "org_id": "$_id",
            "ws": 1,
            "ws_users": 1,
            "ws_learners": 1,
            "ws_editors": 1,
            "_id": 0,
            "year_month": {"$literal": minute_str}
        }},
        {"$merge": {
            "into": {"db": REPORT_DB_NAME, "coll": ORG_STATUS_COLLECTION},
            "on": ["org_id", "year_month"],
            "whenMatched": "merge",
            "whenNotMatched": "discard"
        }}
    ]
    await source_db.command({"aggregate": "workspaces", "pipeline": pipeline, "cursor": {}})
    print("Completed enrich workspaces!")
    end_time = datetime.now(timezone.utc)
    print("time for enrich_with_workspaces:", end_time - now_il)

async def enrich_with_skills(minute_str):
    print("\nstart enrich_with_skills")

    now_il = datetime.now(ZoneInfo("Asia/Jerusalem"))
    pipeline = [
        {"$group": {
            "_id": "$org_id",
            "skills": {"$sum": 1},
            "skills_published": {
                "$sum": {"$cond": [{"$eq": ["$status", "published"]}, 1, 0]}
            },
            "skills_draft": {
                "$sum": {"$cond": [{"$eq": ["$status", "draft"]}, 1, 0]}
            },
            "skills_archived": {
                "$sum": {"$cond": [{"$eq": ["$is_archived", True]}, 1, 0]}
            },
            "skills_not_archived": {
                "$sum": {"$cond": [{"$eq": ["$is_archived", False]}, 1, 0]}
            }

        }},
        {"$project": {
            "org_id": "$_id",
            "skills": 1,
            "skills_published": 1,
            "skills_draft": 1,
            "_id": 0,
            "year_month": {"$literal": minute_str},
            "skills_archived": 1,
            "skills_not_archived": 1
        }},
        {"$merge": {
            "into": {"db": REPORT_DB_NAME, "coll": ORG_STATUS_COLLECTION},
            "on": ["org_id", "year_month"],
            "whenMatched": "merge",
            "whenNotMatched": "discard"
        }}
    ]
    await source_db.command({"aggregate": "skills", "pipeline": pipeline, "cursor": {}})
    print("Completed enrich skills!")
    end_time = datetime.now(timezone.utc)
    print("time for enrich_with_skills:", end_time - now_il)

async def enrich_with_users(minute_str):
    print("\nstart enrich_with_users")
    now_il = datetime.now(ZoneInfo("Asia/Jerusalem"))
    pipeline = [
        {"$group": {
            "_id": "$org_id",
            "users": {"$sum": 1},
            "users_owner": {
                "$sum": {"$cond": [{"$eq": ["$role", 1]}, 1, 0]}
            },
            "users_admin": {
                "$sum": {"$cond": [{"$eq": ["$role", 2]}, 1, 0]}
            },
            "users_learners": {
                 "$sum": {"$cond": [{"$in": ["$role", [3, 4]]}, 1, 0]}
            }
        }},
        {"$project": {
            "org_id": "$_id",
            "_id": 0,
            "year_month": {"$literal": minute_str},
            "users": 1,
            "users_owner": 1,
            "users_admin": 1,
            "users_learners": 1
        }},
        {"$merge": {
            "into": {"db": REPORT_DB_NAME, "coll": ORG_STATUS_COLLECTION},
            "on": ["org_id", "year_month"],
            "whenMatched": "merge",
            "whenNotMatched": "discard"
        }}

    ]
    await source_db.command({"aggregate": "users", "pipeline": pipeline, "cursor": {}})
    print("Completed enrich users!")
    end_time = datetime.now(timezone.utc)
    print("time for enrich_with_users:", end_time - now_il)

async def main():
    start_time = datetime.now(timezone.utc)
    minute_str = await create_org_status_monthly()
    await enrich_with_workspaces(minute_str)
    await enrich_with_skills(minute_str)
    await enrich_with_users(minute_str)
    client.close()
    end_time = datetime.now(timezone.utc)
    print(f"\nסהכ זמן הריצה: {end_time - start_time}\n")

if __name__ == "__main__":
    asyncio.run(main())
