# © 2026 NTT DATA Japan Co., Ltd. & NTT InfraNet All Rights Reserved.

"""

PCM_0010_deleteProvider.py

処理名:
    公益事業者情報削除

概要:
    ・削除対象の公益事業者または道路管理者に紐づく公益事業者・道路管理者マスタ、
        ベクタレイヤマスタのデータが更新作業中（運用作業中）でないこと。
        ＜対象マスタ＞
        公益事業者・道路管理者マスタ、ベクタレイヤマスタ
    ・本機能実施前にレイヤ情報削除機能により
        該当公益事業者または道路管理者が提供したレイヤが全て削除されていること。

実行コマンド形式:
    python3 [バッチ格納パス]/PCM_0010_deleteProvider.py
    --provider_code=[公益事業者・道路管理者コード]
"""

import argparse
import traceback

from core.config_reader import read_config
from core.database import Database
from core.logger import LogManager
from core.secretProperties import SecretPropertiesSingleton
from core.validations import Validations
from util.checkProviderExistence import check_provider_existence
from util.getProviderId import get_provider_id

log_manager = LogManager()
logger = log_manager.get_logger("PCM_0010_公益事業者情報削除")
config = read_config(logger)


# secret_nameをconfigから取得し、secret_propsにAWS Secrets Managerの値を格納
secret_name = config["aws"]["secret_name"]
secret_props = SecretPropertiesSingleton(secret_name, config, logger)

# シークレットからマスタ管理スキーマ名を取得
db_mst_schema = secret_props.get("db_mst_schema")


# 起動パラメータを受け取る関数
def parse_args():
    try:
        # 完全一致のみ許可
        parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
        parser.add_argument("--provider_code", required=False)
        return parser.parse_args()
    except Exception as e:
        # コマンドライン引数の解析に失敗した場合
        logger.error("BPE0037", str(e.message))
        logger.process_error_end()


# 1. 入力値チェック
def validate_provider_code(provider_code):
    # 必須パラメータチェック
    if not provider_code:
        logger.error("BPE0018", "公益事業者・道路管理者コード")
        logger.process_error_end()

    # 桁数（1以上20以下）
    if not Validations.is_valid_length(provider_code, 1, 20):
        logger.error("BPE0019", "公益事業者・道路管理者コード", provider_code)
        logger.process_error_end()


# 4. ベクタレイヤマスタのレイヤデータ存在確認
def check_vector_layer_exists(db_connection, provider_code, provider_id):
    # ベクタレイヤマスタにレイヤデータが存在するか確認
    query = (
        f"SELECT EXISTS (SELECT 1 FROM {db_mst_schema}.mst_vector_layer "
        "WHERE provider_id = %s)"
    )
    result = Database.execute_query(
        db_connection, logger, query, (provider_id,), fetchone=True
    )
    if result:
        logger.error("BPE0029", "ベクタレイヤマスタ", provider_code, provider_id)
        logger.process_error_end()


# 5. 公益事業者・道路管理者マスタデータ削除
def delete_provider(db_connection, provider_code):
    # 公益事業者・道路管理者マスタから削除対象のデータを削除
    query = f"DELETE FROM {db_mst_schema}.mst_provider WHERE provider_code = %s"
    Database.execute_query(db_connection, logger, query, (provider_code,), commit=True)
    logger.info("BPI0007", provider_code)


# メイン処理
# 公益事業者・道路管理者情報削除
def main():

    try:
        # 開始ログ出力
        logger.process_start()

        # 起動パラメータの取得
        provider_code = parse_args().provider_code

        # 1. 入力値チェック
        validate_provider_code(provider_code)

        # DB接続を取得
        db_connection = Database.get_mstdb_connection(logger)

        # 2. 公益事業者・道路管理者既存データ確認
        check_provider_existence(db_connection, db_mst_schema, provider_code, logger)

        # 3. 公益事業者・道路管理者ID取得
        provider_id = get_provider_id(
            db_connection, db_mst_schema, provider_code, logger
        )

        # 4. ベクタレイヤマスタのレイヤデータ存在確認
        check_vector_layer_exists(db_connection, provider_code, provider_id)

        # 5. 公益事業者・道路管理者マスタデータ削除
        delete_provider(db_connection, provider_code)

        # 正常終了ログ出力
        logger.process_normal_end()

    except Exception:
        logger.error("BPE0009", traceback.format_exc())
        logger.process_error_end()


if __name__ == "__main__":
    main()
