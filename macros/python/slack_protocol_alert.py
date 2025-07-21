#!/usr/bin/env python3
import json
import sys
import requests
import re
import os
import subprocess

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

def get_protocol_data():
    """Get protocol data by running the query directly"""
    try:
        # Run dbt to execute the query
        result = subprocess.run([
            'dbt', 'run-operation', 'run_custom_query', 
            '--args', '{query: "SELECT modified_timestamp :: DATE AS DATE, blockchain, platform_name AS protocol, defillama_volume_percent FROM silver_metrics__bridge_comparison WHERE defillama_volume_percent > 9.99 qualify RANK() over (ORDER BY modified_timestamp :: DATE DESC) = 1"}'
        ], capture_output=True, text=True)
        
        # Parse the output
        data = []
        for line in result.stdout.split('\n'):
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
        print(f"Error getting protocol data: {e}")
        return []

def main():
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is required")
        sys.exit(1)
    
    # Get data from the query
    data = get_protocol_data()
    
    if not data:
        print("No data found or error parsing query results")
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