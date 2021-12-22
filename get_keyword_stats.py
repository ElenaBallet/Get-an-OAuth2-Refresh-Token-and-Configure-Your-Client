import sys
from src import gads_client, config
from src.models.utils import choose_account_id
from google.ads.googleads.errors import GoogleAdsException


# Put an account id to download stats from. Note: not MCC, no dash lines
CUSTOMER_ID = ""

# [START get_keyword_stats]
def main(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_criterion.criterion_id,
          ad_group_criterion.keyword.text,
          ad_group_criterion.keyword.match_type,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros
        FROM keyword_view WHERE segments.date DURING LAST_7_DAYS
        AND campaign.advertising_channel_type = 'SEARCH'
        AND ad_group.status = 'ENABLED'
        AND ad_group_criterion.status IN ('ENABLED', 'PAUSED')
        ORDER BY metrics.impressions DESC
        LIMIT 50"""

    # Issues a search request using streaming.
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = query
    stream = ga_service.search_stream(search_request)
    for batch in stream:
        for row in batch.results:
            campaign = row.campaign
            ad_group = row.ad_group
            criterion = row.ad_group_criterion
            metrics = row.metrics
            print(
                f'Keyword text "{criterion.keyword.text}" with '
                f'match type "{criterion.keyword.match_type.name}" '
                f"and ID {criterion.criterion_id} in "
                f'ad group "{ad_group.name}" '
                f'with ID "{ad_group.id}" '
                f'in campaign "{campaign.name}" '
                f"with ID {campaign.id} "
                f"had {metrics.impressions} impression(s), "
                f"{metrics.clicks} click(s), and "
                f"{metrics.cost_micros} cost (in micros) during "
                "the last 7 days."
            )
    # [END get_keyword_stats]


if __name__ == "__main__":
    id_to_load = choose_account_id(CUSTOMER_ID, config.get("test_account_id", None))
    print(gads_client, id_to_load)
    try:
        main(gads_client, id_to_load)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)