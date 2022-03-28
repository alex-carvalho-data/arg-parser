import pytest
from arg_parser import (
    parse_default_args, parse_bootstrap_args,parse_raw_to_bronze_args,
    parse_silver_to_gold_args
)
from typing import List


@pytest.mark.parametrize(
    'cmd_line_args,expected_pipeline_run_id,expected_correlation_id',
    [
        pytest.param(
            ['crazy_run_code_123', 'olynse-1619'],
            'crazy_run_code_123',
            'olynse-1619',
            id='all-params-passed'
        )
    ]
)
def test_parse_default_args(cmd_line_args: List[str],
                            expected_pipeline_run_id: str,
                            expected_correlation_id: str) -> None:
    parsed_args = parse_default_args(cmd_line_args)

    assert parsed_args.pipeline_run_id == expected_pipeline_run_id
    assert parsed_args.correlation_id == expected_correlation_id


@pytest.mark.parametrize(
    f'cmd_line_args,'
    f'expected_pipeline_run_id,'
    f'expected_correlation_id,'
    f'expected_storage_account,'
    f'expected_entity,'
    f'expected_table_name',
    [
        pytest.param(
            ['xpto_run_id_456',
             'olynse-1619',
             'storage_x',
             'china',
             'sample_values_table'],
            'xpto_run_id_456',
            'olynse-1619',
            'storage_x',
            'china',
            'sample_values_table',
            id='full-args'
        )
    ]
)
def test_parse_raw_to_bronze_args(cmd_line_args: List[str],
                                  expected_pipeline_run_id: str,
                                  expected_correlation_id: str,
                                  expected_storage_account: str,
                                  expected_entity: str,
                                  expected_table_name: str) -> None:
    parsed_args = parse_raw_to_bronze_args(cmd_line_args)

    assert parsed_args.pipeline_run_id == expected_pipeline_run_id
    assert parsed_args.correlation_id == expected_correlation_id
    assert parsed_args.storage_account == expected_storage_account
    assert parsed_args.entity == expected_entity
    assert parsed_args.table_name == expected_table_name


@pytest.mark.parametrize(f'cmd_line_args,'
                         f'expected_pipeline_run_id,'
                         f'expected_correlation_id,'
                         f'expected_key_gen_service_url,'
                         f'expected_oauth_tenant_id,'
                         f'expected_oauth_client_id,'
                         f'expected_client_secret_key,'
                         f'expected_oauth_scope',
                         [
                             pytest.param(
                                 ['xpto_run_id_456',
                                  'olynse-1619',
                                  'http://keygen.service',
                                  'tenant_tal',
                                  'client_tal',
                                  'secret_x',
                                  'scope_y'],
                                 'xpto_run_id_456',
                                 'olynse-1619',
                                 'http://keygen.service',
                                 'tenant_tal',
                                 'client_tal',
                                 'secret_x',
                                 'scope_y',
                                 id='all-params'
                             )
                         ])
def test_parse_silver_to_gold(cmd_line_args: List[str],
                              expected_pipeline_run_id: str,
                              expected_correlation_id: str,
                              expected_key_gen_service_url: str,
                              expected_oauth_tenant_id: str,
                              expected_oauth_client_id: str,
                              expected_client_secret_key: str,
                              expected_oauth_scope: str) -> None:
    parsed_args = parse_silver_to_gold_args(cmd_line_args)

    assert parsed_args.pipeline_run_id == expected_pipeline_run_id
    assert parsed_args.correlation_id == expected_correlation_id
    assert parsed_args.key_gen_service_url == expected_key_gen_service_url
    assert parsed_args.oauth_tenant_id == expected_oauth_tenant_id
    assert parsed_args.oauth_client_id == expected_oauth_client_id
    assert parsed_args.client_secret_key == expected_client_secret_key
    assert parsed_args.oauth_scope == expected_oauth_scope


@pytest.mark.parametrize(f'cmd_line_args,'
                         f'expected_storage_account,'
                         f'expected_correlation_id,'
                         f'expected_bronze_overwrite_schema,'
                         f'expected_silver_overwrite_schema,'
                         f'expected_gold_overwrite_schema',
                         [
                             pytest.param(
                                 [
                                     'storage_x',
                                     'olynse_1619',
                                     'True',
                                     'False',
                                     'True'
                                 ],
                                 'storage_x',
                                 'olynse_1619',
                                 True,
                                 False,
                                 True,
                                 id='all-args'
                             )
                         ])
def test_parse_bootstrap_args(cmd_line_args,
                              expected_storage_account: str,
                              expected_correlation_id: str,
                              expected_bronze_overwrite_schema: bool,
                              expected_silver_overwrite_schema: bool,
                              expected_gold_overwrite_schema: bool):
    parsed_args = parse_bootstrap_args(cmd_line_args)

    assert parsed_args.storage_account == expected_storage_account
    assert parsed_args.correlation_id == expected_correlation_id
    assert (parsed_args.bronze_overwrite_schema
            == expected_bronze_overwrite_schema)
    assert (parsed_args.silver_overwrite_schema
            == expected_silver_overwrite_schema)
    assert parsed_args.gold_overwrite_schema == expected_gold_overwrite_schema
