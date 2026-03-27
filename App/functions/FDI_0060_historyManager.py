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

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
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
    invalid_ids = [iid for iid in import_ids if not re.match(r"^[0-9]+$", iid)]
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
        if not result:
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
    # S3クライアント作成(リトライ設定を強化)
    retry_config = Config(
        retries={
            "max_attempts": 5,  # 最大リトライ回数を5回に設定
            "mode": "adaptive",  # アダプティブリトライモード
        },
        connect_timeout=60,  # 接続タイムアウト: 60秒
        read_timeout=300,  # 読み取りタイムアウト: 300秒(5分)
        max_pool_connections=50,  # コネクションプール数
    )
    s3 = boto3.client("s3", region_name=AWS_REGION, config=retry_config)

    # TransferConfig設定(マルチパートアップロードの最適化)
    transfer_config = TransferConfig(
        multipart_threshold=1024 * 1024 * 50,  # 50MB以上でマルチパート
        multipart_chunksize=1024 * 1024 * 50,  # チャンクサイズ50MB
        max_concurrency=10,  # 並行アップロード数
        use_threads=True,  # マルチスレッド使用
    )

    # アップロード済み取込IDリスト
    uploaded_import_ids = []

    for import_id in import_ids:
        fac_data_master_table_name = fac_tables.get(import_id)
        key = f"{fac_data_master_table_name}/dump_{import_id}.dump"
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
        env["PGPASSWORD"] = secret_props.get("db_pass")

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
                # S3へアップロード(TransferConfigを使用してマルチパート対応)
                s3.upload_file(
                    tmpfile.name, history_bucket_name, key, Config=transfer_config
                )
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

    pattern = re.compile(r"dump_(?P<import_id>.+)\.dump$")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    success = True
    for import_id in import_ids:
        fac_data_master_table_name = fac_tables.get(import_id)
        prefix = f"{fac_data_master_table_name}/"

        stored_dumps = []
        next_token = None
        try:
            while True:
                params = {"Bucket": history_bucket_name, "Prefix": prefix}
                if next_token:
                    params["ContinuationToken"] = next_token
                response = s3.list_objects_v2(**params)
                for obj in response.get("Contents", []):
                    key = obj.get("Key")
                    match = pattern.search(key)
                    if not match:
                        continue
                    stored_dumps.append((match.group("import_id"), key))
                if not response.get("IsTruncated"):
                    break
                next_token = response.get("NextContinuationToken")
            if not stored_dumps:
                continue

            sorted_dumps = sorted(
                stored_dumps, key=lambda info: _sorting_value(info[0])
            )
            keep_count_eff = min(keep_count, len(sorted_dumps))
            to_delete = (
                sorted_dumps[: len(sorted_dumps) - keep_count_eff]
                if keep_count_eff
                else sorted_dumps
            )

            for _, key in to_delete:
                s3.delete_object(Bucket=history_bucket_name, Key=key)
        except Exception:
            errorkey = f"{fac_data_master_table_name}/dump_{import_id}.dump"
            logger.warning("BPW0027", import_id, errorkey)
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
        key = f"{fac_data_master_table_name}/dump_{uploaded_import_id}.dump"
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
