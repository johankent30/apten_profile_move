import streamlit as st
import pandas as pd
import io
import time
from datetime import datetime
import requests
from typing import Dict, Optional, Tuple, List

# Page config
st.set_page_config(
    page_title="Apten Profile Switcher",
    page_icon="🔄",
    layout="centered"
)

# Constants
API_BASE_URL = "https://api.attent.app/v1"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 1

class AptenAPIStreamlit:
    def __init__(self, api_key: str):
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }
        self.lookup_endpoint = f"{API_BASE_URL}/leads/lookup"
        self.switch_profile_endpoint = f"{API_BASE_URL}/leads/{{leadId}}/switchCustomerProfile"
        self.get_lead_endpoint = f"{API_BASE_URL}/leads/{{leadId}}"
        self.remove_tags_endpoint = f"{API_BASE_URL}/leads/{{leadId}}/tags/remove"
    
    def _make_request_with_retry(self, method: str, url: str, **kwargs) -> Tuple[bool, Optional[dict], str]:
        """Make an HTTP request with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=self.headers, timeout=REQUEST_TIMEOUT, **kwargs)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=self.headers, timeout=REQUEST_TIMEOUT, **kwargs)
                else:
                    return False, None, f"Unsupported HTTP method: {method}"
                
                if response.status_code == 200:
                    try:
                        return True, response.json(), ""
                    except ValueError:
                        return False, None, "Invalid JSON response"
                
                elif response.status_code == 401:
                    return False, None, "Unauthorized - check your API key"
                elif response.status_code == 404:
                    return False, None, "Lead not found"
                elif response.status_code == 429:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * 2)
                        continue
                    return False, None, "Rate limited"
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return False, None, error_msg
                    
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return False, None, "Request timed out"
            except requests.exceptions.ConnectionError:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                return False, None, "Connection error"
            except Exception as e:
                return False, None, f"Unexpected error: {str(e)}"
        
        return False, None, f"Failed after {MAX_RETRIES} attempts"
    
    def lookup_lead(self, phone: str) -> Tuple[bool, Optional[str], str]:
        """Lookup a lead by phone number."""
        params = {"phone": phone}
        success, data, error = self._make_request_with_retry('GET', self.lookup_endpoint, params=params)
        
        if success and data:
            lead_id = data.get('id', '')
            if lead_id:
                return True, lead_id, ""
            else:
                # Log the actual response for debugging
                return False, None, f"No lead ID in response. Response: {str(data)[:100]}"
        
        return False, None, error
    
    def switch_profile(self, lead_id: str, target_profile: str) -> Tuple[bool, str]:
        """Switch a lead's customer profile."""
        url = self.switch_profile_endpoint.format(leadId=lead_id)

        payload = {
            "profile": target_profile,
            "sendMessage": True,
            "messageDelayHours": 0,
            "messageDelayMins": 0,
            "clearMemory": False
        }

        success, data, error = self._make_request_with_retry('POST', url, json=payload)

        if success:
            return True, ""
        else:
            return False, error

    def get_lead_tag_ids(self, lead_id: str) -> Tuple[bool, List[str], str]:
        """Fetch a lead and return its current tag IDs."""
        url = self.get_lead_endpoint.format(leadId=lead_id)
        success, data, error = self._make_request_with_retry('GET', url)

        if not success:
            return False, [], error

        tags = data.get('tags', []) if data else []
        tag_ids = [t.get('id') for t in tags if t.get('id')]
        return True, tag_ids, ""

    def remove_tags(self, lead_id: str, tag_ids: List[str]) -> Tuple[bool, str]:
        """Remove specific tags from a lead."""
        url = self.remove_tags_endpoint.format(leadId=lead_id)
        success, data, error = self._make_request_with_retry('POST', url, json={"tagIds": tag_ids})

        if success:
            return True, ""
        else:
            return False, error

    def process_lead(self, lead_data: Dict[str, str]) -> Dict[str, str]:
        """Process a single lead: switch profile, then remove all current tags."""
        phone = lead_data['phone']
        target_profile = lead_data['target_profile']

        result = {
            'lead_id': '',
            'status': 'FAILED',
            'profile_status': 'FAILED',
            'tag_removal_status': '',
            'tags_removed_count': 0,
            'error_message': '',
        }

        # Lookup lead
        success, lead_id, error = self.lookup_lead(phone)
        if not success:
            result['error_message'] = f"Lookup failed: {error}"
            return result

        result['lead_id'] = lead_id

        # Switch profile
        success, error = self.switch_profile(lead_id, target_profile)
        if not success:
            result['error_message'] = f"Profile switch failed: {error}"
            return result

        result['profile_status'] = 'SUCCESS'

        # Fetch current tags
        success, tag_ids, error = self.get_lead_tag_ids(lead_id)
        if not success:
            result['status'] = 'PARTIAL'
            result['tag_removal_status'] = 'FAILED'
            result['error_message'] = f"Profile switched but failed to fetch tags: {error}"
            return result

        # Remove tags if any
        if not tag_ids:
            result['status'] = 'SUCCESS'
            result['tag_removal_status'] = 'N/A'
            return result

        success, error = self.remove_tags(lead_id, tag_ids)
        if not success:
            result['status'] = 'PARTIAL'
            result['tag_removal_status'] = 'FAILED'
            result['error_message'] = f"Profile switched but tag removal failed: {error}"
            return result

        result['status'] = 'SUCCESS'
        result['tag_removal_status'] = 'SUCCESS'
        result['tags_removed_count'] = len(tag_ids)
        return result

def process_csv(df: pd.DataFrame, api_key: str):
    """Process the uploaded CSV file."""
    api = AptenAPIStreamlit(api_key)
    
    # Prepare results
    results = []
    
    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_rows = len(df)
    successful_count = 0
    partial_count = 0
    failed_count = 0

    # Process each row
    for idx, row in df.iterrows():
        # Clean phone number
        phone = ''.join(filter(str.isdigit, str(row.get('Mobile Phone', ''))))

        if not phone:
            failed_count += 1
            results.append({
                'Row Number': idx + 2,
                'First Name': row.get('First Name', ''),
                'Last Name': row.get('Last Name', ''),
                'Phone': row.get('Mobile Phone', ''),
                'Target Profile': row.get('Customer Profile', ''),
                'Lead ID': '',
                'Status': 'FAILED',
                'Profile Switch': 'FAILED',
                'Tag Removal': '',
                'Tags Removed': 0,
                'Error Message': 'Invalid phone number'
            })
            continue

        # Get target profile
        target_profile = row.get('Customer Profile', '').strip()
        if not target_profile:
            target_profile = row.get('Customer Profile - MOVE', '').strip()

        if not target_profile:
            failed_count += 1
            results.append({
                'Row Number': idx + 2,
                'First Name': row.get('First Name', ''),
                'Last Name': row.get('Last Name', ''),
                'Phone': phone,
                'Target Profile': '',
                'Lead ID': '',
                'Status': 'FAILED',
                'Profile Switch': 'FAILED',
                'Tag Removal': '',
                'Tags Removed': 0,
                'Error Message': 'No target profile specified'
            })
            continue

        lead_data = {
            'phone': phone,
            'target_profile': target_profile,
            'first_name': row.get('First Name', ''),
            'last_name': row.get('Last Name', '')
        }

        # Update status
        lead_name = f"{lead_data['first_name']} {lead_data['last_name']}".strip()
        status_text.text(f"Processing {idx + 1}/{total_rows}: {lead_name}")

        # Process the lead
        outcome = api.process_lead(lead_data)

        if outcome['status'] == 'SUCCESS':
            successful_count += 1
        elif outcome['status'] == 'PARTIAL':
            partial_count += 1
        else:
            failed_count += 1

        results.append({
            'Row Number': idx + 2,
            'First Name': lead_data['first_name'],
            'Last Name': lead_data['last_name'],
            'Phone': phone,
            'Target Profile': target_profile,
            'Lead ID': outcome['lead_id'],
            'Status': outcome['status'],
            'Profile Switch': outcome['profile_status'],
            'Tag Removal': outcome['tag_removal_status'],
            'Tags Removed': outcome['tags_removed_count'],
            'Error Message': outcome['error_message']
        })

        # Update progress
        progress = (idx + 1) / total_rows
        progress_bar.progress(progress)

        # Small delay to avoid rate limiting
        if idx < total_rows - 1:
            time.sleep(0.1)

    # Clear progress indicators
    progress_bar.empty()
    status_text.empty()

    return results, successful_count, partial_count, failed_count

def main():
    st.title("🔄 Apten Profile Switcher")
    st.markdown("Upload a CSV file to switch customer profiles and clear all tags in bulk")
    st.info("✨ **v1.1** — now also removes **all existing tags** from each lead after switching their profile.")

    # Instructions
    with st.expander("📋 Instructions", expanded=False):
        st.markdown("""
        1. **Enter your API Key**: Your Apten API key for authentication
        2. **Upload CSV File**: Must contain columns:
           - First Name
           - Last Name
           - Mobile Phone
           - Customer Profile (or Customer Profile - MOVE)
        3. **Click Process**: For each lead, the tool will:
           - Switch their customer profile
           - Remove **all** tags currently on the lead
        4. **Download Results**: Get a detailed log of each step per lead
        """)
    
    # API Key input
    api_key = st.text_input(
        "API Key",
        type="password",
        placeholder="Enter your Apten API key",
        help="Your API key will not be stored and is only used for this session"
    )
    
    # File upload
    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type=['csv'],
        help="Select the CSV file containing leads to process"
    )
    
    if uploaded_file and api_key:
        # Read CSV
        try:
            df = pd.read_csv(uploaded_file)
            
            # Show preview
            st.subheader("📊 File Preview")
            st.write(f"Total rows: {len(df)}")
            st.dataframe(df.head(), use_container_width=True)
            
            # Check required columns
            required_columns = ['First Name', 'Last Name', 'Mobile Phone']
            profile_columns = ['Customer Profile', 'Customer Profile - MOVE']
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            has_profile_column = any(col in df.columns for col in profile_columns)
            
            if missing_columns:
                st.error(f"❌ Missing required columns: {', '.join(missing_columns)}")
            elif not has_profile_column:
                st.error("❌ Missing profile column. Need either 'Customer Profile' or 'Customer Profile - MOVE'")
            else:
                # Process button
                if st.button("🚀 Process Leads", type="primary"):
                    st.markdown("---")
                    st.subheader("Processing...")
                    
                    start_time = datetime.now()
                    
                    # Process the CSV
                    results, successful_count, partial_count, failed_count = process_csv(df, api_key)

                    end_time = datetime.now()
                    duration = end_time - start_time

                    # Show summary
                    st.success("✅ Processing Complete!")

                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Total Processed", len(results))
                    with col2:
                        st.metric("Successful", successful_count)
                    with col3:
                        st.metric("Partial", partial_count)
                    with col4:
                        st.metric("Failed", failed_count)
                    with col5:
                        st.metric("Duration", str(duration).split('.')[0])

                    # Create results dataframe
                    results_df = pd.DataFrame(results)

                    # Show partial leads if any (profile switched but tag removal failed)
                    if partial_count > 0:
                        st.warning(f"⚠️ {partial_count} leads had profile switched but tag removal failed")
                        partial_df = results_df[results_df['Status'] == 'PARTIAL']
                        st.dataframe(partial_df, use_container_width=True)

                    # Show failed leads if any
                    if failed_count > 0:
                        st.warning(f"⚠️ {failed_count} leads failed to process")
                        failed_df = results_df[results_df['Status'] == 'FAILED']
                        st.dataframe(failed_df, use_container_width=True)
                    
                    # Download button for results
                    csv_buffer = io.StringIO()
                    results_df.to_csv(csv_buffer, index=False)
                    csv_data = csv_buffer.getvalue()
                    
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"profile_switch_results_{timestamp}.csv"
                    
                    st.download_button(
                        label="📥 Download Results",
                        data=csv_data,
                        file_name=filename,
                        mime="text/csv"
                    )
                    
        except Exception as e:
            st.error(f"❌ Error reading CSV file: {str(e)}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
        Apten Profile Switcher v1.1
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
