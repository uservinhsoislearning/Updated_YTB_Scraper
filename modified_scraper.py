import requests
import sys
import time
import os
import argparse

# List of features to collect
snippet_features = ["title", "publishedAt", "channelId", "channelTitle"]

# Characters to exclude
unsafe_characters = ['\n', '"']

# Column headers
header = ["video_id"] + snippet_features + ["category", "trending_date", "tags", "duration", "view_count", "likes", "dislikes",
                                            "comment_count", "subscriber_count", "engagement_rate", "thumbnail_link", "description", "region_restriction"]

# Category mapping
category_mapping = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
    "30": "Movies",
    "31": "Anime/Animation",
    "32": "Action/Adventure",
    "33": "Classics",
    "34": "Comedy",
    "35": "Documentary",
    "36": "Drama",
    "37": "Family",
    "38": "Foreign",
    "39": "Horror",
    "40": "Sci-Fi/Fantasy",
    "41": "Thriller",
    "42": "Shorts",
    "43": "Shows",
    "44": "Trailers"
}

def setup(api_path, code_path):
    with open(api_path, 'r') as file:
        api_key = file.readline().strip()

    with open(code_path) as file:
        country_codes = [x.strip() for x in file]

    return api_key, country_codes

def prepare_feature(feature):
    feature = str(feature).replace('"', '""')  # Escape quotes for CSV
    return f'"{feature}"'

def api_request(api_key, page_token, country_code):
    request_url = (
        f"https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics,contentDetails&chart=mostPopular"
        f"&regionCode={country_code}&maxResults=50"
        f"&key={api_key}"
    )
    if page_token:
        request_url += f"&pageToken={page_token}"
    
    request = requests.get(request_url)

    if request.status_code == 429:
        print("Temp-banned due to excess requests, please wait and continue later")
        sys.exit()
    elif request.status_code != 200:
        print(f"Error: {request.status_code} - {request.text}")
        return {}

    return request.json()

def get_tags(tags_list):
    return prepare_feature("|".join(tags_list))

def get_subscriber_count(api_key, channel_id):
    url = (
        f"https://www.googleapis.com/youtube/v3/channels"
        f"?part=statistics&id={channel_id}&key={api_key}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['items'][0]['statistics'].get('subscriberCount', 0)
    else:
        print(f"Error fetching subscriber count: {response.status_code}")
        return 0

def get_videos(api_key, items):
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        if "statistics" not in video or "contentDetails" not in video:
            continue

        video_id = prepare_feature(video['id'])
        snippet = video['snippet']
        statistics = video['statistics']
        content_details = video['contentDetails']

        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]

        category_id = snippet.get("categoryId", "")
        category = category_mapping.get(category_id, "Unknown")

        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", {}).get("default", {}).get("url", "")
        trending_date = time.strftime("%y.%d.%m")
        tags = get_tags(snippet.get("tags", ["[none]"]))
        
        view_count = int(statistics.get("viewCount", 0))

        likes = int(statistics.get('likeCount', 0))
        comment_count = int(statistics.get('commentCount', 0))

        if 'likeCount' not in statistics:
            ratings_disabled = True
            likes = 0

        if 'commentCount' not in statistics:
            comments_disabled = True
            comment_count = 0
        
        channel_id = snippet.get("channelId", "")
        subscriber_count = get_subscriber_count(api_key, channel_id)
        engagement_rate = (likes + comment_count) / view_count if view_count else 0
        duration = content_details.get('duration', '')
        region_restriction = content_details.get('regionRestriction', {}).get('allowed', 'None')

        line = [video_id] + features + [prepare_feature(category)] + [prepare_feature(x) for x in [
            trending_date, tags, duration, view_count, likes, 0,  # Dislikes are no longer available
            comment_count, subscriber_count, engagement_rate, thumbnail_link, description, region_restriction
        ]]
        lines.append(",".join(line))
    return lines

def get_pages(api_key, country_code):
    country_data = []
    next_page_token = ""

    while next_page_token is not None:
        video_data_page = api_request(api_key, next_page_token, country_code)
        next_page_token = video_data_page.get("nextPageToken", None)

        items = video_data_page.get('items', [])
        country_data += get_videos(api_key, items)

    return country_data

def write_to_file(output_dir, country_code, country_data):
    print(f"Writing {country_code} data to file...")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = os.path.join(output_dir, f"{time.strftime('%y.%d.%m')}_{country_code}_videos.csv")
    with open(file_path, "w+", encoding='utf-8') as file:
        for row in country_data:
            file.write(f"{row}\n")

def get_data(api_key, country_codes, output_dir):
    for country_code in country_codes:
        country_data = [",".join(header)] + get_pages(api_key, country_code)
        write_to_file(output_dir, country_code, country_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--key_path', help='Path to the file containing the api key', default='api_key.txt')
    parser.add_argument('--country_code_path', help='Path to the file containing the list of country codes', default='country_codes.txt')
    parser.add_argument('--output_dir', help='Path to save the outputted files', default='output/')

    args = parser.parse_args()

    api_key, country_codes = setup(args.key_path, args.country_code_path)
    get_data(api_key, country_codes, args.output_dir)