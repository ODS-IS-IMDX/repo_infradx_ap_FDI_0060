# © 2026 NTT DATA Japan Co., Ltd. & NTT InfraNet All Rights Reserved.

"""
FDI_0060_historyManager.py

処理名:
    履歴管理

概要:
    ・設備データ管理マスタDBから設備データのダンプファイルを取得し、履歴管理用ストレージにアップロードする。
    ・ストレージはS3を使用し、現行+1世代分の履歴を保管する。


実行コマンド形式:
    python3 [バッチ格納先パス]/FDI_0060_historyManager.py
    --import_id=[取込ID]
"""

import argparse
import os
import re
import subprocess
import tempfile
import traceback
from collections import Counter

import boto3
from core.config_reader import read_config
from core.database import Database
from core.logger import LogManager
from core.message import get_message
from core.secretProperties import SecretPropertiesSingleton
from util.getImportManagementTableName import get_import_management_table_name
from util.updateImportManagement import update_import_management

log_manager = LogManager()
logger = log_manager.get_logger("FDI_0060_履歴管理")
config = read_config(logger)

# secret_nameをconfigから取得し、secret_propsにAWS Secrets Managerの値を格納
secret_name = config["aws"]["secret_name"]
secret_props = SecretPropertiesSingleton(secret_name, config, logger)


# シークレットから設備データスキーマ名を取得
db_fac_schema = secret_props.get("db_fac_schema")
# シークレットマネージャーから履歴管理用ストレージ(S3)バケット名を取得
history_bucket_name = secret_props.get("history_bucket_name")

AWS_REGION = config["aws"]["region"].strip()
CODE_LIST = {
    "import_id": "取込ID",
}


# 起動パラメータを受け取る関数
def parse_args():
    try:
        # 完全一致のみ許可
        parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
        parser.add_argument("--import_id", required=False)
        return parser.parse_args()
    except Exception as e:
        # コマンドライン引数の解析に失敗した場合
        logger.error("BPE0037", str(e.message))
        logger.process_error_end()


# 1.入力値チェック
def validate_inputs(import_id_param):

    # 起動パラメータが設定されているか確認
    if not import_id_param:
        logger.error("BPE0018", CODE_LIST["import_id"])
        logger.process_error_end()

    import_ids = [
        value.strip() for value in import_id_param.split(",") if value.strip()
    ]
    if not import_ids:
        logger.error("BPE0018", CODE_LIST["import_id"])
        logger.process_error_end()

    # フォーマットチェック
    invalid_ids = [iid for iid in import_ids if not re.match(r"^[0-9_]+$", iid)]
    if invalid_ids:
        # a.取込管理テーブル更新
        update_import_management_for_deletion(
            import_ids,
            get_message("BPE0019").format("取込ID", import_ids),
        )
        logger.error("BPE0019", "取込ID", import_ids)
        logger.process_error_end()

    return import_ids


# 2. 取込管理内テーブル名取得
def get_import_management_tables(import_ids):
    db_connection = Database.get_mstdb_connection(logger)
    fac_tables = {}
    query = (
        "SELECT EXISTS(SELECT * "
        "FROM pg_tables "
        "WHERE schemaname = %s"
        " AND tablename = %s)"
    )

    for import_id in import_ids:
        fac_data_master_table_name = get_import_management_table_name(
            db_connection, logger, import_id
        ).get("fac_data_master_table_name")

        result = Database.execute_query(
            db_connection,
            logger,
            query,
            params=(db_fac_schema, fac_data_master_table_name),
            fetchone=True,
        )
        if not result or not result[0]:
            # a.取込管理テーブル更新
            update_import_management_for_deletion(
                import_ids,
                get_message("BPE0043").format(import_id, fac_data_master_table_name),
            )
            logger.error("BPE0043", import_id, fac_data_master_table_name)
            logger.process_error_end()

        fac_tables[import_id] = fac_data_master_table_name

    return fac_tables


# 3. 設備データのダンプファイル取得・アップロード
def upload_fac_dump_files(import_ids, fac_tables):
    # S3クライアント作成
    s3 = boto3.client("s3", region_name=AWS_REGION)
    # アップロード済み取込IDリスト
    uploaded_import_ids = []

    for import_id in import_ids:
        fac_data_master_table_name = fac_tables.get(import_id)
        key = f"{fac_data_master_table_name}/dump_{import_id}.dmp"
        cmd = [
            "pg_dump",
            "-h",
            secret_props.get("db_host"),
            "-p",
            secret_props.get("db_port"),
            "-U",
            secret_props.get("db_user"),
            "-d",
            secret_props.get("db_name"),
            "-t",
            f"{db_fac_schema}.{fac_data_master_table_name}",
            "-F",
            "c",
        ]

        # 環境変数にパスワードを設定
        env = os.environ.copy()
        env["PGPASSWORD"] = secret_props.get("db_password")

        with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
            try:
                subprocess.run(cmd + ["-f", tmpfile.name], check=True, env=env)
            except Exception:
                # b.アップロード済みのダンプファイル削除
                delete_uploaded_dump_file(uploaded_import_ids, fac_tables)
                # a.取込管理テーブル更新
                update_import_management_for_deletion(
                    import_ids,
                    get_message("BPE0065").format(
                        import_id, fac_data_master_table_name
                    ),
                )
                logger.error("BPE0065", import_id, fac_data_master_table_name)
                logger.process_error_end()

            try:
                s3.upload_file(tmpfile.name, history_bucket_name, key)
                uploaded_import_ids.append(import_id)
            except Exception:
                # b.アップロード済みのダンプファイル削除
                delete_uploaded_dump_file(uploaded_import_ids, fac_tables)
                # a.取込管理テーブル更新
                update_import_management_for_deletion(
                    import_ids,
                    get_message("BPE0066").format(
                        import_id, fac_data_master_table_name
                    ),
                )
                logger.error("BPE0066", import_id, fac_data_master_table_name)
                logger.process_error_end()


# 4. ダンプファイル削除
def delete_fac_dump_file(import_ids, fac_tables, keep_count=2):
    if not import_ids:
        return True

    def _sorting_value(import_id: str):
        cleaned = import_id.replace("_", "")
        return int(cleaned) if cleaned.isdigit() else cleaned

    sorted_ids = sorted(import_ids, key=_sorting_value)
    keep_slice = sorted_ids[-min(keep_count, len(sorted_ids))]
    keep_counter = Counter(keep_slice)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    success = True
    for import_id in import_ids:
        if keep_counter.get(import_id, 0) > 0:
            keep_counter[import_id] -= 1
            continue

        fac_data_master_table_name = fac_tables.get(import_id)

        key = f"{fac_data_master_table_name}/dump_{import_id}.dmp"
        try:
            s3.delete_object(Bucket=history_bucket_name, Key=key)
        except Exception:
            logger.warning("BPW0027", import_id, key)
            success = False

    return success


# a.取込管理テーブル更新
def update_import_management_for_deletion(import_ids, error_detail):
    db_connection = Database.get_mstdb_connection(logger)
    for import_id in import_ids:
        update_import_management(
            db_connection,
            logger,
            import_id,
            "95",
            error_detail,
            None,
            None,
            None,
        )


# b.アップロード済みのダンプファイル削除
def delete_uploaded_dump_file(uploaded_import_ids, fac_tables):
    if not uploaded_import_ids:
        return

    # S3クライアント作成
    s3 = boto3.client("s3", region_name=AWS_REGION)
    for uploaded_import_id in uploaded_import_ids:
        fac_data_master_table_name = fac_tables.get(uploaded_import_id)
        key = f"{fac_data_master_table_name}/dump_{uploaded_import_id}.dmp"
        try:
            s3.delete_object(Bucket=history_bucket_name, Key=key)
        except Exception:
            logger.warning("BPW0028", uploaded_import_id, key)


def main():
    import_ids = []
    fac_tables = {}
    try:
        # 開始ログ出力
        logger.process_start()

        # 起動パラメータの取得
        args = parse_args()

        # 1. 共通入力値チェック
        import_ids = validate_inputs(args.import_id)

        # 2. 取込管理内テーブル名取得
        fac_tables = get_import_management_tables(import_ids)

        # 3.設備データのダンプファイル取得・アップロード
        upload_fac_dump_files(import_ids, fac_tables)

        # 4.ダンプファイル削除
        warn = not delete_fac_dump_file(import_ids, fac_tables)

        # 5.終了コード返却
        if warn:
            logger.process_warning_end()
        else:
            logger.process_normal_end()

    except Exception:
        # a.取込管理テーブル更新
        update_import_management_for_deletion(
            import_ids,
            get_message("BPE0009").format(traceback.format_exc()),
        )
        logger.error("BPE0009", traceback.format_exc())
        logger.process_error_end()


if __name__ == "__main__":
    main()
