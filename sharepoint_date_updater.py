# This script updates the 'date added' within the point history file to match that on the point's sharepoint page.

from requests_ntlm import HttpNtlmAuth
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
from getpass import getpass
import os
import json
from urllib.parse import quote

class SharePointPointUpdater:
    def __init__(self, base_url, site_name, library_name, username, password, domain=None, backup_dir=None, output_dir=None):
        self.base_url = base_url.rstrip('/')
        self.site_name = site_name
        self.library_name = library_name
        self.username = username
        self.password = password
        self.domain = domain
        self.session = requests.Session()
        
        if domain:
            auth = HttpNtlmAuth(f'{domain}\\{username}', password)
        else:
            auth = HttpNtlmAuth(username, password)
        self.session.auth = auth
        
        self.headers = {
            'Accept': 'application/json;odata=verbose'
        }
        
        # Set default directories or use provided ones
        self.backup_dir = backup_dir or os.path.join(os.path.expanduser('~'), 'SharePointUpdater', 'backups')
        self.output_dir = output_dir or os.path.join(os.path.expanduser('~'), 'SharePointUpdater', 'downloads')
        os.makedirs(self.backup_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def get_points_by_date_range(self, start_date, end_date=None):
        """Get all points within a date range using SharePoint REST API"""
        try:
            date_added_field = 'Date_x0020_Added'
            start_date_iso = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if end_date:
                end_date_iso = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                date_filter = f"{date_added_field} ge datetime'{start_date_iso}' and {date_added_field} le datetime'{end_date_iso}'"
            else:
                date_filter = f"{date_added_field} ge datetime'{start_date_iso}'"

            encoded_filter = quote(date_filter)
            
            api_url = (f"{self.base_url}/sites/{self.site_name}/_api/web/lists/getbytitle('{self.library_name}')/items"
                      f"?$select=ID,{date_added_field},FileLeafRef,Title&$filter={encoded_filter}&$top=5000")
            
            print(f"\nQuerying points from SharePoint...")
            response = self.session.get(api_url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            points = data.get('d', {}).get('results', [])
            
            point_list = []
            for item in points:
                point_number = item.get('FileLeafRef', '') or item.get('Title', '')
                if point_number.startswith('Point '):
                    point_number = point_number[6:]
                point_list.append({
                    'id': item['ID'],
                    'date_added': item.get(date_added_field),
                    'point_number': point_number
                })
            
            return point_list
            
        except Exception as e:
            print(f"Failed to query points: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response text: {e.response.text}")
            return []

    def get_text_file_name(self, point_number):
        """Get the name of the text file in the point's folder"""
        try:
            folder_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFolderByServerRelativeUrl('/sites/{self.site_name}/{self.library_name}/{point_number}')/Files"
            
            response = self.session.get(folder_url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            files = data.get('d', {}).get('results', [])
            
            for file in files:
                file_name = file.get('Name', '')
                if file_name.lower().endswith('.txt'):
                    print(f"Found text file: {file_name}")
                    return file_name
                    
            print("No text file found in folder")
            return None
            
        except Exception as e:
            print(f"Failed to get text file name: {str(e)}")
            return None

    def get_text_file_content(self, point_number, file_name):
        """Get the content of the text file from SharePoint"""
        try:
            file_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFileByServerRelativeUrl('/sites/{self.site_name}/{self.library_name}/{point_number}/{file_name}')/$value"
            
            download_headers = {'Accept': 'text/plain'}
            response = self.session.get(file_url, headers=download_headers)
            response.raise_for_status()
            return response.text
            
        except Exception as e:
            print(f"Failed to get text file content: {str(e)}")
            return None

    def download_original_file(self, point_number, file_name):
        """Download and save the original file before making any changes"""
        try:
            content = self.get_text_file_content(point_number, file_name)
            if content is None:
                return False

            point_backup_dir = os.path.join(self.backup_dir, f"Point_{point_number}")
            os.makedirs(point_backup_dir, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file_name = f"original_{file_name.replace('.txt', '')}_{timestamp}.txt"
            backup_path = os.path.join(point_backup_dir, backup_file_name)

            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(content)

            print(f"Original file backed up to: {backup_path}")
            return True

        except Exception as e:
            print(f"Failed to backup original file: {str(e)}")
            return False

    def rename_sharepoint_file(self, point_number, old_file_name, new_file_name):
        """Rename a file in SharePoint using MoveTo operation"""
        try:
            old_file_url = f'/sites/{self.site_name}/{self.library_name}/{point_number}/{old_file_name}'
            new_file_url = f'/sites/{self.site_name}/{self.library_name}/{point_number}/{new_file_name}'
            
            move_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFileByServerRelativeUrl('{old_file_url}')/MoveTo(newUrl='{new_file_url}',flags=1)"
            
            headers = {
                'Accept': 'application/json;odata=verbose',
                'Content-Type': 'application/json;odata=verbose',
                'X-HTTP-Method': 'POST'
            }
            
            response = self.session.post(move_url, headers=headers)
            response.raise_for_status()
            
            print(f"Successfully renamed file to: {new_file_name}")
            return True
            
        except Exception as e:
            print(f"Failed to rename file: {str(e)}")
            return False

    def update_text_file(self, point_number, file_name, new_date):
        """Update dates for entries where CMS is the initialed user and rename if needed"""
        try:
            if not self.download_original_file(point_number, file_name):
                print("Failed to backup original file, aborting update")
                return False

            content = self.get_text_file_content(point_number, file_name)
            if content is None:
                return False

            desired_file_name = f"Point {point_number}.txt"
            rename_needed = file_name.lower() != desired_file_name.lower()
            
            if isinstance(new_date, str):
                new_date = datetime.strptime(new_date.split('T')[0], '%Y-%m-%d')
            
            new_date_str = new_date.strftime('%m/%d/%Y')
            
            lines = content.split('\n')
            updated_lines = []
            updates_made = 0
            
            for line in lines:
                if 'CMS' in line and re.search(r'\d{2}/\d{2}/\d{4}', line):
                    updated_line = re.sub(
                        r'(\d{2}/\d{2}/\d{4})\s+CMS',
                        f'{new_date_str}\t\tCMS',
                        line
                    )
                    updated_lines.append(updated_line)
                    updates_made += 1
                else:
                    updated_lines.append(line)
            
            if updates_made == 0:
                print("No lines with 'CMS' and a date were found")
                return False
                    
            updated_content = '\n'.join(updated_lines)
            
            if rename_needed:
                if not self.rename_sharepoint_file(point_number, file_name, desired_file_name):
                    print("Failed to rename file, attempting to update content anyway")
                file_name = desired_file_name

            file_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFileByServerRelativeUrl('/sites/{self.site_name}/{self.library_name}/{point_number}/{file_name}')/$value"
            
            headers = {
                'X-HTTP-Method': 'PUT',
                'Content-Type': 'text/plain;charset=utf-8',
                'If-Match': '*'
            }
            
            print(f"Updated {updates_made} date(s) for CMS entries")
            
            response = self.session.put(file_url, data=updated_content.encode('utf-8'), headers=headers)
            response.raise_for_status()
            
            return True
                
        except Exception as e:
            print(f"Failed to update text file: {str(e)}")
            return False

    def process_single_point(self, point_id, point_number, date_added):
        """Process a single point"""
        print(f"\nProcessing point: {point_number} (ID: {point_id})")
        
        file_name = self.get_text_file_name(point_number)
        if not file_name:
            return False

        return self.update_text_file(point_number, file_name, date_added)

    def process_multiple_points(self, start_date, end_date=None, max_points=None):
        """Process multiple points within a date range"""
        points = self.get_points_by_date_range(start_date, end_date)
        
        if not points:
            print("No points found in the specified date range")
            return {'successful': [], 'failed': []}
            
        print(f"\nFound {len(points)} points to process")
        
        if max_points:
            points = points[:max_points]
            print(f"Limited to processing {max_points} points")
        
        results = {
            'successful': [],
            'failed': []
        }
        
        for i, point in enumerate(points, 1):
            point_id = point['id']
            point_number = point['point_number']
            date_added = point['date_added']
            
            print(f"\nProcessing point {i} of {len(points)}")
            print(f"Point Number: {point_number}")
            print(f"SharePoint ID: {point_id}")
            print(f"Date Added: {date_added}")
            
            if self.process_single_point(point_id, point_number, date_added):
                results['successful'].append({'id': point_id, 'number': point_number})
                print(f"Successfully processed point {point_number}")
            else:
                results['failed'].append({'id': point_id, 'number': point_number})
                print(f"Failed to process point {point_number}")
            
            self.save_results(results)
        
        return results

    def save_results(self, results):
        """Save processing results to a JSON file"""
        results_file = os.path.join(self.output_dir, 'processing_results.json')
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)

def main():
    base_url = input("Enter SharePoint base URL: ")
    site_name = input("Enter SharePoint site name: ")
    library_name = input("Enter document library name: ")
    
    username = os.getenv('SHAREPOINT_USERNAME') or input("Enter your username: ")
    domain = os.getenv('SHAREPOINT_DOMAIN') or input("Enter your domain (press Enter if none): ").strip() or None
    password = os.getenv('SHAREPOINT_PASSWORD') or getpass("Enter your password: ")

    start_date_str = input("Enter start date (MM/DD/YYYY): ")
    start_date = datetime.strptime(start_date_str, '%m/%d/%Y')
    
    end_date_str = input("Enter end date (MM/DD/YYYY) or press Enter for no end date: ").strip()
    end_date = datetime.strptime(end_date_str, '%m/%d/%Y') if end_date_str else None
    
    max_points_str = input("Enter maximum number of points to process (or press Enter for no limit): ").strip()
    max_points = int(max_points_str) if max_points_str else None
    
    print("\nInitializing SharePoint connection...")
    updater = SharePointPointUpdater(base_url, site_name, library_name, username, password, domain)
    
    results = updater.process_multiple_points(start_date, end_date, max_points)
    
    print("\nProcessing complete!")
    print(f"Successfully processed: {len(results['successful'])} points")
    print(f"Failed to process: {len(results['failed'])} points")
    print(f"Results saved to: {os.path.join(updater.output_dir, 'processing_results.json')}")
    print(f"Original files backed up to: {updater.backup_dir}")

if __name__ == "__main__":
    main()