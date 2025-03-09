import pandas as pd
import requests

# Load the CSV file
file_path = "2025-01-15_urban_all_urls.csv"
df = pd.read_csv(file_path, header=None, names=["url"])

# Function to check both initial and final URL status codes
def check_url_status(url):
    try:
        # Make the initial HEAD request without following redirects
        initial_response = requests.head(url, allow_redirects=False, timeout=5)
        initial_status = initial_response.status_code

        # If there's a redirect, follow it to get the final response
        if initial_status in [301, 302, 307, 308]:
            final_response = requests.head(url, allow_redirects=True, timeout=5)
            final_status = final_response.status_code
            final_url = final_response.url
        else:
            final_status = initial_status  # No redirect, same status
            final_url = url  # No change in URL

        return initial_status, final_status, final_url

    except requests.RequestException:
        return "Error", "Error", url  # If request fails, return "Error"

# Apply function and store results
df[["initial_status", "final_status", "final_url"]] = df["url"].apply(lambda url: pd.Series(check_url_status(url)))

# Save results to a new CSV file
df.to_csv("url_status_results.csv", index=False)

print("Results saved in 'url_status_results.csv'.")
