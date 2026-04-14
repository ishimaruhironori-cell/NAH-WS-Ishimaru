"""
IS 締め報告 - Slack 自動投稿スクリプト
毎日定時に launchd または GitHub Actions から実行される

環境変数:
  SLACK_WEBHOOK_URL  : Slack Incoming Webhook URL
  GOOGLE_CREDENTIALS : GCP サービスアカウント JSON（GitHub Actions 用）
                       未設定時はアプリケーションデフォルト認証を使用（ローカル用）
"""

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

JST = timezone(timedelta(hours=9))

# IS担当メンバーのSalesforce User ID
IS_MEMBER_IDS = (
    "'005F9000009tfNIIAY',"  # 時任 清美
    "'005F9000009uJGSIA2',"  # 石丸 浩典
    "'005Q900000H5uarIAB',"  # 佐々木 愛香
    "'005Q900000H5uasIAB',"  # 桜井 香乃
    "'005Q900000MZGzhIAH',"  # 岡野 駿人
    "'005Q900000OrL0TIAV',"  # 南野 久美子
    "'005Q900000ZcAlNIAV',"  # 川田 良太
    "'005Q900000gqq2LIAQ',"  # 淤見 嶺人
    "'005Q900000iAWJpIAO',"  # 川尻 夏代
    "'005F9000009tfNHIAY'"   # 藤原 滉平
)


def get_bq_client() -> bigquery.Client:
    """BigQuery クライアントを返す。GOOGLE_APPLICATION_CREDENTIALS または ADC を自動使用"""
    return bigquery.Client(project="nah-data")


def bq(sql: str) -> dict:
    """BigQuery クエリを実行して最初の行を dict で返す"""
    client = get_bq_client()
    rows = list(client.query(sql).result())
    return dict(rows[0]) if rows else {}


def fetch_report_data() -> dict:
    today = datetime.now(JST).date()
    yesterday = today - timedelta(days=1)

    acq = bq(f"""
    SELECT
      COUNT(*)                                                                       AS total,
      COUNTIF(SalesTargetBrand__c = 'NOT A HOTEL（プライマリー）')                     AS primary_total,
      COUNTIF(CMT_OpportunityAcquisitionRoute__c IN ('架電', 'メール'))                AS phone_email,
      COUNTIF(CMT_OpportunityAcquisitionRoute__c = 'immedio経由')                     AS immedio,
      COUNTIF(CMT_OpportunityAcquisitionRoute__c = 'ナーチャリングメール')               AS nurturing,
      COUNTIF(SalesTargetBrand__c = 'NOT A GARAGE')                                 AS garage_total
    FROM `nah-data.salesforce.Opportunity`
    WHERE IsDeleted = false
      AND DATE(CreatedDate, 'Asia/Tokyo') = '{today}'
      AND InsideSalesOwner__c IN ({IS_MEMBER_IDS})
    """)

    mtg = bq(f"""
    SELECT
      COUNTIF(CMT_ScheduledDate__c = '{yesterday}')                                                             AS scheduled,
      COUNTIF(CMT_ScheduledDate__c = '{yesterday}' AND CMT_FirstMeeting__c = '{yesterday}')                     AS conducted,
      COUNTIF(SalesTargetBrand__c = 'NOT A HOTEL（プライマリー）'
              AND CMT_ScheduledDate__c = '{yesterday}')                                                          AS primary_scheduled,
      COUNTIF(SalesTargetBrand__c = 'NOT A HOTEL（プライマリー）'
              AND CMT_ScheduledDate__c = '{yesterday}' AND CMT_FirstMeeting__c = '{yesterday}')                  AS primary_conducted,
      COUNTIF(CMT_OpportunityAcquisitionRoute__c IN ('架電', 'メール')
              AND CMT_ScheduledDate__c = '{yesterday}' AND CMT_FirstMeeting__c = '{yesterday}')                  AS phone_email_conducted,
      COUNTIF(CMT_OpportunityAcquisitionRoute__c = 'immedio経由'
              AND CMT_ScheduledDate__c = '{yesterday}' AND CMT_FirstMeeting__c = '{yesterday}')                  AS immedio_conducted,
      COUNTIF(CMT_OpportunityAcquisitionRoute__c = 'ナーチャリングメール'
              AND CMT_ScheduledDate__c = '{yesterday}' AND CMT_FirstMeeting__c = '{yesterday}')                  AS nurturing_conducted,
      COUNTIF(SalesTargetBrand__c = 'NOT A GARAGE'
              AND CMT_ScheduledDate__c = '{yesterday}')                                                          AS garage_scheduled,
      COUNTIF(SalesTargetBrand__c = 'NOT A GARAGE'
              AND CMT_ScheduledDate__c = '{yesterday}' AND CMT_FirstMeeting__c = '{yesterday}')                  AS garage_conducted
    FROM `nah-data.salesforce.Opportunity`
    WHERE IsDeleted = false
    """)

    return {"today": today, "yesterday": yesterday, "acq": acq, "mtg": mtg}


def build_message(data: dict) -> str:
    today = data["today"]
    a = data["acq"]
    m = data["mtg"]

    scheduled = int(m.get("scheduled") or 0)
    conducted = int(m.get("conducted") or 0)
    rate_pct = f"{conducted / scheduled * 100:.1f}%" if scheduled > 0 else "-%"

    lines = [
        f"*{today}の締め報告です！*",
        "",
        f"💪 *商談獲得: 計{a['total']}件*",
        f"🏠 プライマリー: {a['primary_total']}件",
        f"📞 架電/メール: {a['phone_email']}件",
        f"🟦 immedio: {a['immedio']}件",
        f"✉️ Nurturing Mail: {a['nurturing']}件",
        f"🚗 GARAGE: {a['garage_total']}件",
        "",
        f"🔥 *商談実施（商談実施率: {rate_pct}）*",
        f"商談実施予定: {scheduled}件",
        f"商談実施数: {conducted}件",
        "",
        f"🏠 プライマリー: {m['primary_conducted']}/{m['primary_scheduled']}件",
        f"📞 架電/メール: {m['phone_email_conducted']}件",
        f"🟦 immedio: {m['immedio_conducted']}件",
        f"✉️ Nurturing Mail: {m['nurturing_conducted']}件",
        f"🚗 GARAGE: {m['garage_conducted']}/{m['garage_scheduled']}件",
    ]
    return "\n".join(lines)


def post_to_slack(message: str) -> None:
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as res:
        if res.status != 200:
            raise RuntimeError(f"Slack 投稿失敗: {res.status} {res.read()}")


if __name__ == "__main__":
    data = fetch_report_data()
    message = build_message(data)
    print(message)
    post_to_slack(message)
    print("✅ Slack 投稿完了")
