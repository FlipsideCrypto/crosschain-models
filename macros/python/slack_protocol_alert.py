#!/usr/bin/env python3
import json
import sys
import requests
import re
import os

def create_protocol_checklist(protocols_by_chain):
    if not protocols_by_chain:
        return "✅ *No protocols need curation at this time*"
    
    message = " *Protocols Needing Curation*\n\n"
    
    for chain, protocols in protocols_by_chain.items():
        message += f"*{chain.upper()}*\n"
        for protocol in protocols:
            message += f"• [ ] {protocol['protocol']} ({protocol['defillama_volume_percent']}%)\n"
        message += "\n"
    
    total_protocols = sum(len(protocols) for protocols in protocols_by_chain.values())
    message += f"*Total: {total_protocols} protocols across {len(protocols_by_chain)} chains*"
    
    return message

def send_slack_message(webhook_url, message):
    payload = {"text": message}
    response = requests.post(webhook_url, json=payload)
    return response.status_code == 200

def parse_dbt_output(output_file):
    """Parse the dbt operation output which contains agate.Row format"""
    try:
        with open(output_file, 'r') as f:
            content = f.read()
        
        # Extract the agate.Row lines
        data = []
        for line in content.split('\n'):
            if 'Row: <agate.Row:' in line:
                # Extract the data from the agate.Row format
                match = re.search(r'Row: <agate\.Row: \(([^)]+)\)>', line)
                if match:
                    row_data = match.group(1)
                    # Parse the tuple: (date, blockchain, protocol, volume)
                    parts = row_data.split(', ')
                    
                    # Clean up the parts
                    date = parts[0].replace('datetime.date(', '').replace(')', '')
                    blockchain = parts[1].strip("'")
                    protocol = parts[2].strip("'")
                    volume = parts[3].replace('Decimal(', '').replace(')', '')
                    
                    data.append({
                        'date': date,
                        'blockchain': blockchain,
                        'protocol': protocol,
                        'defillama_volume_percent': volume
                    })
        
        return data
    except Exception as e:
        print(f"Error parsing dbt output: {e}")
        return []

def main():
    if len(sys.argv) != 2:
        print("Usage: python slack_protocol_alert.py <protocols_file>")
        sys.exit(1)
    
    protocols_file = sys.argv[1]
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is required")
        sys.exit(1)
    
    # Parse the dbt output
    data = parse_dbt_output(protocols_file)
    
    if not data:
        print("No data found or error parsing file")
        return
    
    # Group by blockchain
    protocols_by_chain = {}
    for row in data:
        chain = row['blockchain']
        if chain not in protocols_by_chain:
            protocols_by_chain[chain] = []
        protocols_by_chain[chain].append(row)
    
    # Create the message
    message = create_protocol_checklist(protocols_by_chain)
    
    print("=== SLACK MESSAGE PREVIEW ===")
    print(message)
    print("=== END PREVIEW ===")
    
    # Send to Slack
    success = send_slack_message(webhook_url, message)
    
    if success:
        print("✅ Slack message sent successfully!")
    else:
        print("❌ Failed to send Slack message")
        sys.exit(1)

if __name__ == "__main__":
    main()