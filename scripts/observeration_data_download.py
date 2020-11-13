"""
Methods to download the observation data from the iNaturalist API
by Frank

Link to API:
https://api.inaturalist.org/v1/docs/#!/Observations/get_observations

API limits:
Max. 100 request per minute, best 60 requests per minute,
max 10.000 request per day

"""
import requests
import pandas as pd
import numpy as np
from time import sleep

# List of projects to download
PROJECT_IDS_NAMES = {
    18620: 'CNC_London_2018',
    33231: 'CNC_London_2019',
    62227: 'CNC_London_2020',
    10931: 'CNC_Los_Angeles_2017',
    16065: 'CNC_Los_Angeles_2018',
    31997: 'CNC_Los_Angeles_2019',
    62506: 'CNC_Los_Angeles_2020',
    11013: 'CNC_San_Francisco_2017',
    16036: 'CNC_San_Francisco_2018',
    29624: 'CNC_San_Francisco_2019',
    62485: 'CNC_San_Francisco_2020',
}


def download_all_projects(project_ids_names):
    """ Download observations for all specified projects """

    for project_id, save_name in PROJECT_IDS_NAMES.items():
        download_observations(project_id, save_name)


def download_observations(project_id, save_name, save_path='../data/raw/observations_v2/'):
    """ Downloads and saves all observations for a specific project.

    Each city nature challenge in one year and city is one project.
    Observations have to be downloaded in 200 chunks. (There is also a limit
    at 10.000 observations, which is why the ID filter in the request
    has to be increased.)

    Arguments:
    ----------
     - project_id: int or str
        The id of the project. One can find it on iNaturalist by searching for
        and selecting a project, and then checking the active filter (to the
        right) which shows the ID of the selected project.

     - save_name: str
        The name of the saved file, should be the name of the project
        and year (convention for example "CNC London 2018").

     - save_path: str
        Relative path where files are saved.
    """
    data = []
    id_above = 1
    num_total_obs = None

    print("\nStarting download")
    print(f"project id: {project_id}")
    print(f"save name: {save_name}\n")

    while True:
        # Update req string
        req_str = f"https://api.inaturalist.org/v1/observations?project_id={project_id}&per_page=200&id_above={id_above}&order=asc&order_by=id"

        # Download results
        res = requests.get(req_str).json()
        results = res['results']

        # check total number of results (only correct at first request)
        if num_total_obs is None:
            num_total_obs = res['total_results']

        # If no results left, stop download
        no_more_results = len(results) == 0
        if no_more_results:
            break

        # Save data for each result
        for result in results:
            # Some data processing of location
            if result['location']:
                coords = list(result['location'].split(','))
                lon = coords[0]
                lat = coords[1]
            else:
                lon = None
                lat = None

            # All data that is to be saved
            data_result = {
                'id': result['id'],
                'observed_on_string': result['observed_on_string'],
                'time_observed_at': result['time_observed_at'],
                'created_time_zone': result['created_time_zone'],
                'created_at': result['created_at'],
                'updated_at': result['updated_at'],
                'description': result['description'],
                'user_id': result['user']['id'],
                'user_login': result['user']['login'],
                'quality_grade': result['quality_grade'],
                'reviewed_by': result['reviewed_by'],
                'faves_count': result['faves_count'],
                'num_identification_agreements': result["num_identification_agreements"],
                'num_identification_disagreements': result['num_identification_disagreements'],
                'identifications_most_agree': result['identifications_most_agree'],
                'identifications_most_disagree': result['identifications_most_disagree'],
                'captive': result['captive'],
                'place_guess': result['place_guess'],
                'place_ids': result['place_ids'],
                'longitude': lon,
                'latitude': lat,
                'positional_accuracy': result['positional_accuracy'],
                'geoprivacy': result['geoprivacy'],
                'taxon_geoprivacy': result['taxon_geoprivacy'],
                'obscured': result['obscured'],
                'species_guess': result['species_guess'],
            }
            # Additional taxon information if available
            if result['taxon']:
                taxon_info = {
                    'taxon_id': result['taxon']['id'],
                    'taxon_name': result['taxon']['name'],
                    'preferred_common_name': result['taxon']['preferred_common_name'] if 'preferred_common_name' in result['taxon'].keys() else None,
                    'iconic_taxon_name': result['taxon']['iconic_taxon_name'] if 'iconic_taxon_name' in result['taxon'].keys() else None,
                    'taxon_rank': result['taxon']['rank'],
                    'taxon_parent_id': result['taxon']['parent_id'],
                    'taxon_native': result['taxon']['native'],
                    'taxon_endemic': result['taxon']['endemic'],
                    'taxon_threatened': result['taxon']['threatened'],
                    'taxon_search_rank': result['taxon']['universal_search_rank'],
                    'taxon_observations': result['taxon']['observations_count'],
                }
                data_result = {**data_result, **taxon_info}

            # Identifications: Who identified which user
            if result['identifications']:
                identifications = []
                for identification in result['identifications']:
                    identifications.append({
                        'user_id': identification['user']['id'],
                        'category': identification["category"],
                        'disagreement': identification["disagreement"],
                    })
                data_result['identifications'] = identifications
            data.append(data_result)

        # Log some info on the download progress
        num_obs_downloaded = len(data)
        percent_done = num_obs_downloaded / num_total_obs
        print(
            f" - downloaded {percent_done:.1%} ({num_obs_downloaded}/{num_total_obs} observations)")

        # Get highest ID for where to start next request
        id_above = results[-1]['id']

        # Sleep to leave API some rest
        sleep(1)

    # Convert to df
    df = pd.DataFrame(data)
    df = df.replace(np.nan, None).convert_dtypes()  # infer best dype

    # Save df
    save_filepath = save_path + save_name + ".csv"
    print("Saving file ", save_filepath)
    df.to_csv(save_filepath, index=False)

    print("Done\n")


if __name__ == '__main__':
    download_all_projects(PROJECT_IDS_NAMES)
