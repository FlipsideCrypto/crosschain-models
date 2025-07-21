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
        print("Running dbt query...")
        
        # Run the main query
        result = subprocess.run([
            'dbt', 'run-operation', 'run_custom_query', 
            '--args', '{query: "SELECT modified_timestamp :: DATE AS DATE, blockchain, platform_name AS protocol, defillama_volume_percent FROM crosschain.silver_metrics.bridge_comparison WHERE defillama_volume_percent > 9.99 qualify RANK() over (ORDER BY modified_timestamp :: DATE DESC) = 1"}'
        ], capture_output=True, text=True)
        
        print(f"dbt command completed with return code: {result.returncode}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        
        # Debug: Let's see the exact format
        print("=== DEBUG: Raw stdout lines ===")
        for i, line in enumerate(result.stdout.split('\n')):
            print(f"Line {i}: '{line}'")
        print("=== END DEBUG ===")
        
        # Parse the output
        data = []
        # Join all lines and then find all agate.Row patterns
        full_output = result.stdout
        
        # Find all agate.Row patterns in the full output
        row_pattern = r'Row: <agate\.Row: \(([^)]+)\)>'
        matches = re.findall(row_pattern, full_output)
        
        print(f"Found {len(matches)} agate.Row matches")
        
        # If regex fails, try a simpler approach
        if len(matches) == 0:
            print("Regex failed, trying simpler approach...")
            lines = result.stdout.split('\n')
            for line in lines:
                if 'Row: <agate.Row:' in line:
                    print(f"Found line with agate.Row: {line}")
                    # Extract everything between the parentheses
                    start = line.find('(')
                    end = line.rfind(')')
                    if start != -1 and end != -1:
                        row_data = line[start+1:end]
                        print(f"Extracted row data: {row_data}")
                        
                        # Parse the four fields properly
                        # Format: datetime.date(2025, 7, 21), 'blockchain', 'protocol', Decimal('volume')
                        
                        # Find the end of the datetime.date part
                        date_end = row_data.find('),')
                        if date_end != -1:
                            # Extract date part
                            date_part = row_data[:date_end+1]
                            # Extract the rest
                            rest_part = row_data[date_end+2:].strip()
                            
                            # Parse date
                            date_match = re.search(r'datetime\.date\((\d+), (\d+), (\d+)\)', date_part)
                            if date_match:
                                year, month, day = date_match.groups()
                                date = f"{year}-{month}-{day}"
                                
                                # Parse the rest: 'blockchain', 'protocol', Decimal('volume')
                                # Split by comma, but be careful with quotes
                                parts = []
                                current_part = ""
                                in_quotes = False
                                paren_count = 0
                                
                                for char in rest_part:
                                    if char == "'" and (len(current_part) == 0 or current_part[-1] != '\\'):
                                        in_quotes = not in_quotes
                                    elif char == '(' and not in_quotes:
                                        paren_count += 1
                                    elif char == ')' and not in_quotes:
                                        paren_count -= 1
                                    elif char == ',' and not in_quotes and paren_count == 0:
                                        parts.append(current_part.strip())
                                        current_part = ""
                                        continue
                                    current_part += char
                                
                                # Add the last part
                                if current_part.strip():
                                    parts.append(current_part.strip())
                                
                                print(f"Parsed parts: {parts}")
                                
                                if len(parts) >= 3:
                                    blockchain = parts[0].strip("'")
                                    protocol = parts[1].strip("'")
                                    volume = parts[2].replace('Decimal(', '').replace(')', '')
                                    
                                    data.append({
                                        'date': date,
                                        'blockchain': blockchain,
                                        'protocol': protocol,
                                        'defillama_volume_percent': volume
                                    })
                                    print(f"Added data: {data[-1]}")
                                else:
                                    print(f"Not enough parts found: {len(parts)}")
                            else:
                                print(f"Could not parse date from: {date_part}")
                        else:
                            print(f"Could not find date end in: {row_data}")
        
        for i, row_data in enumerate(matches):
            print(f"Processing match {i+1}: {row_data}")
            
            # Split by comma and handle the complex format
            parts = []
            current_part = ""
            in_quotes = False
            paren_count = 0
            
            for char in row_data:
                if char == "'" and (len(current_part) == 0 or current_part[-1] != '\\'):
                    in_quotes = not in_quotes
                elif char == '(' and not in_quotes:
                    paren_count += 1
                elif char == ')' and not in_quotes:
                    paren_count -= 1
                elif char == ',' and not in_quotes and paren_count == 0:
                    parts.append(current_part.strip())
                    current_part = ""
                    continue
                current_part += char
            
            # Add the last part
            if current_part.strip():
                parts.append(current_part.strip())
            
            print(f"Parsed parts: {parts}")
            
            if len(parts) >= 4:
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
                print(f"Added data: {data[-1]}")
            else:
                print(f"Not enough parts found: {len(parts)}")
        
        print(f"Total data rows found: {len(data)}")
        return data
    except Exception as e:
        print(f"Error getting protocol data: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is required")
        sys.exit(1)
    
    # Get data from the query
    data = get_protocol_data()
    
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
    
    # Send to Slack (even if no data)
    success = send_slack_message(webhook_url, message)
    
    if success:
        print("✅ Slack message sent successfully!")
    else:
        print("❌ Failed to send Slack message")
        sys.exit(1)

if __name__ == "__main__":
    main()