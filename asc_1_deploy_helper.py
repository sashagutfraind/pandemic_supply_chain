#!/usr/bin/env python3
"""
AWS Supply Chain Deployment Helper

This script automates the parts that CAN be automated and provides clear
instructions for the manual steps that must be done in the AWS Console.

Usage:
    python supplychain_deploy_helper.py
"""

import boto3
import json
import sys
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_step(step_num: str, title: str):
    """Print a formatted step header."""
    print(f"\n{step_num}. {title}")
    print("-" * 70)


def load_config(config_path: str = "asc_instance_config.json") -> dict:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"✗ Configuration file not found: {config_path}")
        print(f"  Create the file with required settings")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in configuration file: {e}")
        sys.exit(1)


def save_config(config: dict, config_path: str = "asc_instance_config.json"):
    """Save configuration to JSON file."""
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)


def check_identity_center(aws_region: str) -> dict | None:
    """Check if Identity Center is enabled."""
    print_step("STEP 1", "Checking IAM Identity Center")
    
    try:
        sso_admin = boto3.client('sso-admin', region_name=aws_region)
        response = sso_admin.list_instances()
        
        if not response.get('Instances'):
            print(f"✗ IAM Identity Center is NOT enabled")
            print(f"\n📋 To enable Identity Center:")
            print(f"   1. Go to: https://console.aws.amazon.com/singlesignon")
            print(f"   2. Click 'Enable' to activate Identity Center")
            print(f"   3. Follow the setup wizard")
            print(f"   4. Once enabled, run this script again")
            return None
        
        instance = response['Instances'][0]
        print(f"✓ IAM Identity Center is enabled")
        print(f"  Instance ARN: {instance['InstanceArn']}")
        print(f"  Identity Store ID: {instance['IdentityStoreId']}")
        
        # Get account ID
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity()['Account']
        
        return {
            'account_id': account_id,
            'instance_arn': instance['InstanceArn'],
            'identity_store_id': instance['IdentityStoreId']
        }
        
    except ClientError as e:
        print(f"✗ Error accessing Identity Center: {e.response['Error']['Code']}")
        print(f"  Ensure you have permissions to access IAM Identity Center")
        return None


def create_identity_center_user(
    identity_store_id: str,
    username: str,
    email: str,
    first_name: str,
    last_name: str,
    aws_region: str
) -> dict | None:
    """Create user in Identity Center."""
    print_step("STEP 2", f"Creating Identity Center User: {username}")
    
    try:
        identitystore = boto3.client('identitystore', region_name=aws_region)
        
        # Check if user already exists
        response = identitystore.list_users(
            IdentityStoreId=identity_store_id,
            Filters=[{'AttributePath': 'UserName', 'AttributeValue': username}]
        )
        
        if response.get('Users'):
            user = response['Users'][0]
            print(f"✓ User already exists")
            print(f"  User ID: {user['UserId']}")
            print(f"  Username: {username}")
            return user
        
        # Create new user
        response = identitystore.create_user(
            IdentityStoreId=identity_store_id,
            UserName=username,
            DisplayName=f"{first_name} {last_name}",
            Name={'GivenName': first_name, 'FamilyName': last_name},
            Emails=[{'Value': email, 'Type': 'Work', 'Primary': True}]
        )
        
        user_id = response['UserId']
        print(f"✓ User created successfully")
        print(f"  User ID: {user_id}")
        print(f"  Username: {username}")
        print(f"  Email: {email}")
        
        return {'UserId': user_id, 'UserName': username}
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Code']}")
        print(f"  Message: {e.response['Error']['Message']}")
        return None


def create_supply_chain_permission_set(instance_arn: str, aws_region: str) -> str | None:
    """Create custom permission set for AWS Supply Chain."""
    print_step("STEP 3", "Creating AWS Supply Chain Permission Set")
    
    try:
        sso_admin = boto3.client('sso-admin', region_name=aws_region)
        
        # Check if permission set already exists
        response = sso_admin.list_permission_sets(InstanceArn=instance_arn)
        
        for ps_arn in response.get('PermissionSets', []):
            ps_details = sso_admin.describe_permission_set(
                InstanceArn=instance_arn,
                PermissionSetArn=ps_arn
            )
            ps_name = ps_details['PermissionSet'].get('Name', '')
            
            if ps_name == 'AWSSupplyChainFullAccess':
                print(f"✓ Permission set already exists: {ps_name}")
                print(f"  ARN: {ps_arn}")
                return ps_arn
        
        # Create new permission set
        print("Creating custom permission set...")
        
        response = sso_admin.create_permission_set(
            InstanceArn=instance_arn,
            Name='AWSSupplyChainFullAccess',
            Description='Full access to AWS Supply Chain and required services',
            SessionDuration='PT8H'
        )
        
        ps_arn = response['PermissionSet']['PermissionSetArn']
        
        # Define inline policy
        custom_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "SupplyChainFullAccess",
                    "Effect": "Allow",
                    "Action": ["scn:*"],
                    "Resource": "*"
                },
                {
                    "Sid": "S3Access",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                        "s3:ListBucket", "s3:GetBucketLocation"
                    ],
                    "Resource": ["arn:aws:s3:::aws-supply-chain-*", "arn:aws:s3:::aws-supply-chain-*/*"]
                },
                {
                    "Sid": "GlueAccess",
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetPartition*"],
                    "Resource": "*"
                },
                {
                    "Sid": "AthenaAccess",
                    "Effect": "Allow",
                    "Action": ["athena:*QueryExecution", "athena:GetWorkGroup"],
                    "Resource": "*"
                },
                {
                    "Sid": "BedrockAccess",
                    "Effect": "Allow",
                    "Action": ["bedrock:InvokeModel*"],
                    "Resource": "*"
                }
            ]
        }
        
        sso_admin.put_inline_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=ps_arn,
            InlinePolicy=json.dumps(custom_policy)
        )
        
        print(f"✓ Permission set created with Supply Chain access")
        print(f"  ARN: {ps_arn}")
        
        return ps_arn
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Code']}")
        return None


def assign_user_to_account(
    instance_arn: str,
    permission_set_arn: str,
    user_id: str,
    account_id: str,
    aws_region: str
) -> bool:
    """Assign user to AWS account with permission set."""
    print_step("STEP 4", "Assigning User to AWS Account")
    
    try:
        sso_admin = boto3.client('sso-admin', region_name=aws_region)
        
        # Check if assignment exists
        response = sso_admin.list_account_assignments(
            InstanceArn=instance_arn,
            AccountId=account_id,
            PermissionSetArn=permission_set_arn
        )
        
        for assignment in response.get('AccountAssignments', []):
            if (assignment.get('PrincipalType') == 'USER' and 
                assignment.get('PrincipalId') == user_id):
                print(f"✓ User already assigned to account")
                return True
        
        # Create assignment
        response = sso_admin.create_account_assignment(
            InstanceArn=instance_arn,
            TargetId=account_id,
            TargetType='AWS_ACCOUNT',
            PermissionSetArn=permission_set_arn,
            PrincipalType='USER',
            PrincipalId=user_id
        )
        
        print(f"✓ User assigned to AWS account")
        return True
        
    except ClientError as e:
        print(f"✗ Error: {e.response['Error']['Code']}")
        return False


def provide_manual_instructions(config: dict, user: dict, app_config: dict):
    """Provide instructions for manual instance creation."""
    print_step("STEP 5", "Manual Instance Creation (Required)")
    
    aws_region = app_config['aws_region']
    identity_store_id = config['identity_store_id']
    admin_user = app_config['admin_user']
    
    print("\n⚠️  The following steps MUST be done manually in AWS Console:")
    print("   AWS Supply Chain instance creation cannot be fully automated")
    
    print(f"\n📋 Manual Setup Instructions:")
    print(f"\n   1. CREATE INSTANCE:")
    print(f"      a. Go to: https://console.aws.amazon.com/scn/home?region={aws_region}")
    print(f"      b. Click 'Create instance -> Set up application as admin'")
    print(f"      c. Click 'Create in Advanced Setup'")
    print(f"      d. We recommend to add AWS KMS Key, as it might help data import'")
    print(f"      e. Wait for instance to become Active (~2-3 minutes)")
    
    print(f"\n   2. CONFIGURE IDENTITY SOURCE:")
    print(f"      a. Select your instance")
    print(f"      b. Click 'Settings' in left navigation")
    print(f"      c. Under 'Identity source' → Click 'Configure'")
    print(f"      d. Select: IAM Identity Center")
    print(f"      e. Identity Store ID: {identity_store_id}")
    print(f"      f. Click 'Save'")
    
    print(f"\n   3. ADD ADMIN USER:")
    print(f"      a. Still in Settings → Scroll to 'User access'")
    print(f"      b. Click 'Add user'")
    print(f"      c. Search for: {admin_user['first_name']} {admin_user['last_name']}")
    print(f"      d. Or search by email: {admin_user['email']}")
    print(f"      e. Select user and assign Role: Admin")
    print(f"      f. Click 'Add'")
    
    print(f"\n   4. COMPLETE SETUP:")
    print(f"      a. Look for 'Complete setup' banner/button")
    print(f"      b. Click 'Complete setup'")
    print(f"      c. Wait for completion (~30 seconds)")
    
    print(f"\n   5. SET USER PASSWORD:")
    print(f"      a. Go to IAM Identity Center console")
    print(f"      b. Users → {admin_user['username']} → Reset password")
    print(f"      c. Generate one-time password")
    print(f"      d. Share with user securely")
    
    print(f"\n⏸️  Complete these steps, then press Enter to continue...")
    input()


def update_config_with_instance():
    """Prompt user to update config with instance name."""
    print_step("STEP 6", "Update Configuration")
    
    print("\n📝 Please update asc_instance_config.json with your instance details:")
    print(f"\n   Add or update the 'instance_name' and 'instance_id' field with your info")
    print(f"   (You can find this in the Supply Chain console)")
    
    print(f"\n   Example:")
    print(f'   "instance_id": "a291e5e8-c257-4e02-af1e-48745a81fadd", "instance_name": "j7bym7y8"')
    
    print(f"\n⏸️  Update the config file, then press Enter to continue...")
    input()
    
    # Reload config
    try:
        config = load_config()
        if config.get('instance_id'):
            print(f"\n✓ Configuration updated with instance ID: {config['instance_id']}")
            return config
        else:
            print(f"\n⚠️  Warning: instance_id not found in config")
            return config
    except Exception as e:
        print(f"\n⚠️  Could not reload config: {e}")
        return None


def save_deployment_info(config: dict, user: dict, app_config: dict):
    """Save deployment information."""
    print_step("FINAL", "Saving Deployment Information")
    
    Path("output-data").mkdir(exist_ok=True)
    
    output_file = "output-data/supplychain_deployment.json"
    deployment_info = {
        'deployed_at': datetime.now().isoformat(),
        'region': app_config['aws_region'],
        'account_id': config['account_id'],
        'identity_center': {
            'instance_arn': config['instance_arn'],
            'identity_store_id': config['identity_store_id'],
            'access_portal_url': f"https://{config['identity_store_id']}.awsapps.com/start"
        },
        'admin_user': {
            'username': app_config['admin_user']['username'],
            'user_id': user['UserId'],
            'email': app_config['admin_user']['email']
        },
        'instance_id': app_config.get('instance_id', 'NOT_SET'),
        'automated_steps': [
            'Identity Center user created',
            'Custom permission set created',
            'User assigned to AWS account'
        ],
        'manual_steps_completed': [
            'Instance created in console',
            'Identity source configured',
            'User added to instance',
            'Setup completed',
            'User password set'
        ]
    }
    
    with open(output_file, 'w') as f:
        json.dump(deployment_info, f, indent=2)
    
    print(f"✓ Deployment info saved to: {output_file}")


def main():
    """Main deployment helper flow."""
    print_section("AWS Supply Chain Deployment Helper")
    
    # Load configuration
    app_config = load_config()
    aws_region = app_config['aws_region']
    admin_user = app_config['admin_user']
    
    print(f"\nRegion: {aws_region}")
    print(f"Admin User: {admin_user['username']}")
    
    # Step 1: Check Identity Center
    config = check_identity_center(aws_region)
    if not config:
        print("\n❌ Please enable Identity Center and run this script again")
        sys.exit(1)
    
    # Step 2: Create user
    user = create_identity_center_user(
        config['identity_store_id'],
        admin_user['username'],
        admin_user['email'],
        admin_user['first_name'],
        admin_user['last_name'],
        aws_region
    )
    if not user:
        print("\n❌ Failed to create user")
        sys.exit(1)
    
    # Step 3: Create permission set
    permission_set_arn = create_supply_chain_permission_set(config['instance_arn'], aws_region)
    if not permission_set_arn:
        print("\n❌ Failed to create permission set")
        sys.exit(1)
    
    # Step 4: Assign user to account
    success = assign_user_to_account(
        config['instance_arn'],
        permission_set_arn,
        user['UserId'],
        config['account_id'],
        aws_region
    )
    if not success:
        print("\n❌ Failed to assign user to account")
        sys.exit(1)
    
    # Step 5: Provide manual instructions
    provide_manual_instructions(config, user, app_config)
    
    # Step 6: Update config with instance info
    updated_config = update_config_with_instance()
    if updated_config:
        app_config = updated_config
    
    # Save deployment info
    save_deployment_info(config, user, app_config)
    
    # Final summary
    print_section("✅ DEPLOYMENT HELPER COMPLETE")
    
    print(f"\n📋 What was automated:")
    print(f"  ✓ Identity Center user created")
    print(f"  ✓ Custom permission set created")
    print(f"  ✓ User assigned to AWS account")
    
    print(f"\n📋 What you did manually:")
    print(f"  ✓ Created Supply Chain instance")
    print(f"  ✓ Configured identity source")
    print(f"  ✓ Added user to instance")
    print(f"  ✓ Completed setup")
    print(f"  ✓ Set user password")
    
    print(f"\n🔗 Access Portal:")
    print(f"  https://{config['identity_store_id']}.awsapps.com/start")
    
    print(f"\n✨ Your AWS Supply Chain instance is ready to use!")


if __name__ == "__main__":
    main()
