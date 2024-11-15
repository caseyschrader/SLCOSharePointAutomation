# this script creates a point history text file using a pre-established template and filling in data from a CSV and the point's sharepoint page.

from requests_ntlm import HttpNtlmAuth
import requests
from pathlib import Path
import csv
from datetime import datetime
from dataclasses import dataclass
import logging
from typing import Dict, List, Optional, Tuple
import os
from urllib.parse import quote
from getpass import getpass

@dataclass
class VRSObservation:
    document_num: str
    work_order: str
    control_used: str
    point_number: str
    township_range: str
    section: str
    date_observed: str
    monument_type: Optional[str] = None

class SharePointVRSUpdater:
    def __init__(self, base_url: str, site_name: str, library_name: str, username: str, password: str, 
                 domain: Optional[str] = None, monument_permit: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.site_name = site_name
        self.library_name = library_name
        self.monument_permit = monument_permit
        self.session = self._initialize_session(username, password, domain)
        self._setup_logging()

    def _initialize_session(self, username: str, password: str, domain: Optional[str]) -> requests.Session:
        session = requests.Session()
        auth = HttpNtlmAuth(
            f'{domain}\\{username}' if domain else username,
            password
        )
        session.auth = auth
        session.headers.update({'Accept': 'application/json;odata=verbose'})
        return session

    def _setup_logging(self):
        log_dir = Path.home() / "SharePointUpdater" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / 'sharepoint_updater.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_monument_type(self, point_number: str) -> Optional[str]:
        try:
            list_url = f"{self.base_url}/sites/{self.site_name}/_api/web/lists/getbytitle('{self.library_name}')/items"
            filter_query = f"?$filter=Title eq '{point_number}'&$select=Mon_x0020_Description"
            
            self.logger.info(f"Fetching monument description for point {point_number}")
            response = self.session.get(list_url + filter_query)
            response.raise_for_status()
            
            data = response.json()
            results = data['d']['results']
            
            if results:
                monument_type = results[0].get('Mon_x0020_Description')
                if monument_type:
                    self.logger.info(f"Found monument description for point {point_number}: {monument_type}")
                    return monument_type
            
            self.logger.warning(f"No monument description found for point {point_number}")
            return None
                
        except Exception as e:
            self.logger.error(f"Error fetching monument description for point {point_number}: {str(e)}")
            if hasattr(e, 'response'):
                self.logger.error(f"Response status code: {e.response.status_code}")
                self.logger.error(f"Response text: {e.response.text}")
            return None

    def _create_point_history_content(self, obs: VRSObservation, observer: str, initials: str, 
                                    existing_content: Optional[str] = None) -> str:
        current_date = datetime.now().strftime('%m/%d/%Y')
        permit_text = f"{self.monument_permit} " if self.monument_permit else ""
        location_text = f"Section {obs.section}, {obs.township_range}"
        monument_type_text = f" ({obs.monument_type})" if obs.monument_type else ""
        
        new_entry = f"\n\n{current_date}\t{initials}\tThis monument{monument_type_text} was observed with VRS ({obs.document_num}) using {obs.control_used} and Geoid18"
        new_entry += f"\n\t\t\tas part of WO {obs.work_order}. The purpose of this work order was to facilitate {permit_text}"
        new_entry += f"\n\t\t\t{location_text}. Monument observed by {observer} on {obs.date_observed}.\n\n"
        
        if existing_content:
            return existing_content.rstrip() + new_entry
        
        template = f"""
    POINT HISTORY FILE: for point {obs.point_number}

(mm/dd/yyyy) \t(initials)\t\tACTION/REMARKS
-----------------------------------------------------------------------------------------
{new_entry}"""
        return template

    def get_existing_content(self, point_number: str) -> Optional[str]:
        try:
            folder_path = quote(f'/sites/{self.site_name}/{self.library_name}/{point_number}')
            folder_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFolderByServerRelativeUrl('{folder_path}')/Files"
            
            self.logger.info(f"Checking folder: {folder_url}")
            response = self.session.get(folder_url)
            response.raise_for_status()
            
            files = response.json()['d']['results']
            text_file = next((f for f in files if f['Name'].lower().endswith('.txt')), None)
            
            if text_file:
                self.logger.info(f"Found file: {text_file['Name']}")
                file_path = quote(f'/sites/{self.site_name}/{self.library_name}/{point_number}/{text_file["Name"]}')
                file_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFileByServerRelativeUrl('{file_path}')/$value"
                
                self.logger.info(f"Fetching content from: {file_url}")
                response = self.session.get(file_url, headers={'Accept': 'text/plain'})
                response.raise_for_status()
                
                return response.text
                
            self.logger.info(f"No text file found for point {point_number}")
            return None
                
        except Exception as e:
            self.logger.error(f"Error getting existing content for point {point_number}: {str(e)}")
            if hasattr(e, 'response'):
                self.logger.error(f"Response status code: {e.response.status_code}")
                self.logger.error(f"Response text: {e.response.text}")
            return None

    def update_point_history(self, obs: VRSObservation, observer: str, initials: str) -> bool:
        try:
            obs.monument_type = self.get_monument_type(obs.point_number)
            existing_content = self.get_existing_content(obs.point_number)
            content = self._create_point_history_content(obs, observer, initials, existing_content)
            
            folder_path = quote(f'/sites/{self.site_name}/{self.library_name}/{obs.point_number}')
            
            if existing_content:
                folder_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFolderByServerRelativeUrl('{folder_path}')/Files"
                response = self.session.get(folder_url)
                response.raise_for_status()
                
                files = response.json()['d']['results']
                text_file = next((f for f in files if f['Name'].lower().endswith('.txt')), None)
                
                if not text_file:
                    raise Exception("Text file not found in folder")
                    
                file_path = quote(f'/sites/{self.site_name}/{self.library_name}/{obs.point_number}/{text_file["Name"]}')
                update_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFileByServerRelativeUrl('{file_path}')/$value"
                
                self.logger.info(f"Updating file at: {update_url}")
                headers = {
                    'Content-Type': 'text/plain;charset=utf-8',
                    'X-HTTP-Method': 'PUT',
                    'If-Match': '*'
                }
                response = self.session.put(update_url, data=content.encode('utf-8'), headers=headers)
            else:
                create_url = f"{self.base_url}/sites/{self.site_name}/_api/web/GetFolderByServerRelativeUrl('{folder_path}')/Files/Add(url='Point {obs.point_number}.txt')"
                
                self.logger.info(f"Creating new file at: {create_url}")
                headers = {
                    'Content-Type': 'text/plain;charset=utf-8'
                }
                response = self.session.post(create_url, data=content.encode('utf-8'), headers=headers)
            
            response.raise_for_status()
            self.logger.info(f"Successfully processed point history for point {obs.point_number}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process point history for point {obs.point_number}: {str(e)}")
            if hasattr(e, 'response'):
                self.logger.error(f"Response status code: {e.response.status_code}")
                self.logger.error(f"Response text: {e.response.text}")
            return False

    def process_vrs_csv(self, csv_path: Path, observer: str, initials: str) -> Dict[str, bool]:
        results = {}
        
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                reader.fieldnames = [field.strip() for field in reader.fieldnames]
                
                for row in reader:
                    if not any(row.values()):
                        continue
                    
                    try:
                        cleaned_row = {k.strip(): v.strip() for k, v in row.items()}
                        required_fields = ['DOCUMENT_NUM', 'WORK_ORDER', 'CONTROL_USED', 
                                         'PNT_OBSERVED', 'Township_Range', 'Section', 'Date_Observed']
                        
                        if all(cleaned_row.get(field) for field in required_fields):
                            obs = VRSObservation(
                                document_num=cleaned_row['DOCUMENT_NUM'],
                                work_order=cleaned_row['WORK_ORDER'],
                                control_used=cleaned_row['CONTROL_USED'],
                                point_number=cleaned_row['PNT_OBSERVED'],
                                township_range=cleaned_row['Township_Range'],
                                section=cleaned_row['Section'],
                                date_observed=cleaned_row['Date_Observed']
                            )
                            
                            success = self.update_point_history(obs, observer, initials)
                            results[obs.point_number] = success
                            
                    except Exception as e:
                        point_number = cleaned_row.get('PNT_OBSERVED', 'unknown')
                        self.logger.error(f"Error processing row for point {point_number}: {str(e)}")
                        results[point_number] = False
                        
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            
        return results

def main():
    try:
        base_url = input("Enter SharePoint base URL: ")
        site_name = input("Enter SharePoint site name: ")
        library_name = input("Enter document library name: ")
        
        username = os.getenv('SHAREPOINT_USERNAME') or input("Enter your username: ")
        domain = os.getenv('SHAREPOINT_DOMAIN') or input("Enter your domain (press Enter if none): ").strip() or None
        password = os.getenv('SHAREPOINT_PASSWORD') or getpass("Enter your password: ")

        csv_path = Path(input("Enter the path to your VRS CSV file: ").strip('"'))
        observer = input("Enter observer name: ")
        initials = input("Enter initials: ").strip()
        monument_permit = input("Enter monument permit number (press Enter if none): ").strip() or None
        
        updater = SharePointVRSUpdater(base_url, site_name, library_name, username, password, domain, monument_permit)
        results = updater.process_vrs_csv(csv_path, observer, initials)
        
        print("\nProcessing complete!")
        successful = sum(1 for success in results.values() if success)
        print(f"Successfully processed: {successful} points")
        print(f"Failed to process: {len(results) - successful} points")
        print(f"Check the log file for detailed results")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        logging.error(f"Application error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()