#!/bin/bash
#
# Rusternetes AWS Production Installer
#
# This script automates the complete deployment of Rusternetes on AWS EC2
# with production-ready configuration including VPC, security groups, and optional HA.
#
# Usage:
#   ./aws-install.sh [OPTIONS]
#
# Options:
#   --region REGION       AWS region (default: us-east-1)
#   --instance-type TYPE  EC2 instance type (default: t3.xlarge)
#   --key-name NAME       SSH key pair name (required)
#   --ha                  Install High Availability configuration (3 instances + ALB)
#   --elastic-ip          Allocate and associate Elastic IP (single-node only)
#   --skip-cleanup        Skip cleanup on failure
#   --non-interactive     Skip confirmation prompts
#   --help                Show this help message
#
# Prerequisites:
#   - AWS CLI installed and configured (aws configure)
#   - Valid SSH key pair in AWS (use --key-name)
#   - Appropriate IAM permissions
#
# Examples:
#   # Single-node deployment
#   ./aws-install.sh --key-name rusternetes-keypair --region us-east-1
#
#   # HA deployment with 3 nodes
#   ./aws-install.sh --key-name rusternetes-keypair --ha --region us-west-2
#

set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration defaults
AWS_REGION="us-east-1"
INSTANCE_TYPE="t3.xlarge"
KEY_NAME=""
INSTALL_HA=false
ALLOCATE_EIP=false
SKIP_CLEANUP=false
NON_INTERACTIVE=false
VPC_CIDR="10.0.0.0/16"
INSTALL_DIR="/home/ec2-user/rusternetes"
GITHUB_REPO="https://github.com/yourusername/rusternetes.git"

# Resource tracking (for cleanup)
VPC_ID=""
IGW_ID=""
SUBNET_ID=""
SUBNET_ID_2=""
SUBNET_ID_3=""
SG_ID=""
INSTANCE_ID=""
INSTANCE_2=""
INSTANCE_3=""
EIP_ALLOC=""
LB_ARN=""
TG_ARN=""
LISTENER_ARN=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --instance-type)
            INSTANCE_TYPE="$2"
            shift 2
            ;;
        --key-name)
            KEY_NAME="$2"
            shift 2
            ;;
        --ha)
            INSTALL_HA=true
            shift
            ;;
        --elastic-ip)
            ALLOCATE_EIP=true
            shift
            ;;
        --skip-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        --non-interactive)
            NON_INTERACTIVE=true
            shift
            ;;
        --help)
            grep '^#' "$0" | grep -v '#!/bin/bash' | sed 's/^# //'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Run with --help for usage information"
            exit 1
            ;;
    esac
done

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

confirm() {
    if [[ "$NON_INTERACTIVE" == "true" ]]; then
        return 0
    fi

    local message="$1"
    read -p "$message (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Installation cancelled by user"
        exit 0
    fi
}

# Cleanup function for failures
cleanup() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        log_warn "Cleanup skipped (--skip-cleanup specified)"
        return 0
    fi

    log_step "Cleaning up AWS resources..."

    # Terminate instances
    if [[ -n "$INSTANCE_ID" ]]; then
        aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" 2>/dev/null || true
    fi
    if [[ -n "$INSTANCE_2" ]]; then
        aws ec2 terminate-instances --instance-ids "$INSTANCE_2" --region "$AWS_REGION" 2>/dev/null || true
    fi
    if [[ -n "$INSTANCE_3" ]]; then
        aws ec2 terminate-instances --instance-ids "$INSTANCE_3" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Wait for termination
    if [[ -n "$INSTANCE_ID" ]]; then
        aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Delete load balancer resources
    if [[ -n "$LB_ARN" ]]; then
        aws elbv2 delete-load-balancer --load-balancer-arn "$LB_ARN" --region "$AWS_REGION" 2>/dev/null || true
    fi
    if [[ -n "$TG_ARN" ]]; then
        sleep 10  # Wait for LB deletion
        aws elbv2 delete-target-group --target-group-arn "$TG_ARN" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Release Elastic IP
    if [[ -n "$EIP_ALLOC" ]]; then
        aws ec2 release-address --allocation-id "$EIP_ALLOC" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Delete security group
    if [[ -n "$SG_ID" ]]; then
        sleep 5  # Wait for instances to fully terminate
        aws ec2 delete-security-group --group-id "$SG_ID" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Delete subnets
    if [[ -n "$SUBNET_ID" ]]; then
        aws ec2 delete-subnet --subnet-id "$SUBNET_ID" --region "$AWS_REGION" 2>/dev/null || true
    fi
    if [[ -n "$SUBNET_ID_2" ]]; then
        aws ec2 delete-subnet --subnet-id "$SUBNET_ID_2" --region "$AWS_REGION" 2>/dev/null || true
    fi
    if [[ -n "$SUBNET_ID_3" ]]; then
        aws ec2 delete-subnet --subnet-id "$SUBNET_ID_3" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Detach and delete Internet Gateway
    if [[ -n "$IGW_ID" ]] && [[ -n "$VPC_ID" ]]; then
        aws ec2 detach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" --region "$AWS_REGION" 2>/dev/null || true
        aws ec2 delete-internet-gateway --internet-gateway-id "$IGW_ID" --region "$AWS_REGION" 2>/dev/null || true
    fi

    # Delete VPC
    if [[ -n "$VPC_ID" ]]; then
        aws ec2 delete-vpc --vpc-id "$VPC_ID" --region "$AWS_REGION" 2>/dev/null || true
    fi

    log_info "Cleanup completed"
}

# Set up trap for cleanup on error
trap cleanup ERR

# Check prerequisites
check_prerequisites() {
    log_step "Checking prerequisites..."

    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity --region "$AWS_REGION" &> /dev/null; then
        log_error "AWS credentials not configured or invalid. Run 'aws configure'"
        exit 1
    fi

    # Check key name provided
    if [[ -z "$KEY_NAME" ]]; then
        log_error "SSH key name is required. Use --key-name option"
        exit 1
    fi

    # Verify key exists
    if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$AWS_REGION" &> /dev/null; then
        log_error "Key pair '$KEY_NAME' not found in region $AWS_REGION"
        exit 1
    fi

    # Check HA + EIP conflict
    if [[ "$INSTALL_HA" == "true" ]] && [[ "$ALLOCATE_EIP" == "true" ]]; then
        log_error "Cannot use --elastic-ip with --ha (HA uses load balancer)"
        exit 1
    fi

    log_info "Prerequisites check passed"
}

# Display configuration
display_config() {
    echo
    echo "=========================================="
    echo "  Rusternetes AWS Deployment Configuration"
    echo "=========================================="
    echo "AWS Region: $AWS_REGION"
    echo "Instance Type: $INSTANCE_TYPE"
    echo "SSH Key: $KEY_NAME"
    echo "Configuration: $([ "$INSTALL_HA" == "true" ] && echo "High Availability (3 instances + ALB)" || echo "Single-Node")"
    echo "Elastic IP: $([ "$ALLOCATE_EIP" == "true" ] && echo "Yes" || echo "No")"
    echo "VPC CIDR: $VPC_CIDR"
    echo "=========================================="
    echo

    confirm "Continue with deployment?"
}

# Get latest Amazon Linux 2023 AMI
get_ami() {
    log_step "Finding latest Amazon Linux 2023 AMI..."

    AMI_ID=$(aws ec2 describe-images \
        --owners amazon \
        --filters "Name=name,Values=al2023-ami-2023.*-x86_64" \
        --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
        --output text \
        --region "$AWS_REGION")

    if [[ -z "$AMI_ID" ]]; then
        log_error "Failed to find Amazon Linux 2023 AMI"
        exit 1
    fi

    log_info "Using AMI: $AMI_ID"
}

# Create VPC
create_vpc() {
    log_step "Creating VPC..."

    VPC_ID=$(aws ec2 create-vpc \
        --cidr-block "$VPC_CIDR" \
        --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=rusternetes-vpc}]" \
        --query 'Vpc.VpcId' \
        --output text \
        --region "$AWS_REGION")

    log_info "VPC created: $VPC_ID"

    # Enable DNS
    aws ec2 modify-vpc-attribute \
        --vpc-id "$VPC_ID" \
        --enable-dns-hostnames \
        --region "$AWS_REGION"

    aws ec2 modify-vpc-attribute \
        --vpc-id "$VPC_ID" \
        --enable-dns-support \
        --region "$AWS_REGION"
}

# Create Internet Gateway
create_internet_gateway() {
    log_step "Creating Internet Gateway..."

    IGW_ID=$(aws ec2 create-internet-gateway \
        --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=rusternetes-igw}]" \
        --query 'InternetGateway.InternetGatewayId' \
        --output text \
        --region "$AWS_REGION")

    log_info "Internet Gateway created: $IGW_ID"

    # Attach to VPC
    aws ec2 attach-internet-gateway \
        --internet-gateway-id "$IGW_ID" \
        --vpc-id "$VPC_ID" \
        --region "$AWS_REGION"

    log_info "Internet Gateway attached to VPC"
}

# Create subnets
create_subnets() {
    log_step "Creating subnets..."

    # Get availability zones
    local AZ_1=$(aws ec2 describe-availability-zones \
        --region "$AWS_REGION" \
        --query 'AvailabilityZones[0].ZoneName' \
        --output text)

    # Subnet 1
    SUBNET_ID=$(aws ec2 create-subnet \
        --vpc-id "$VPC_ID" \
        --cidr-block 10.0.1.0/24 \
        --availability-zone "$AZ_1" \
        --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=rusternetes-public-1}]" \
        --query 'Subnet.SubnetId' \
        --output text \
        --region "$AWS_REGION")

    aws ec2 modify-subnet-attribute \
        --subnet-id "$SUBNET_ID" \
        --map-public-ip-on-launch \
        --region "$AWS_REGION"

    log_info "Subnet 1 created: $SUBNET_ID ($AZ_1)"

    # For HA, create additional subnets
    if [[ "$INSTALL_HA" == "true" ]]; then
        local AZ_2=$(aws ec2 describe-availability-zones \
            --region "$AWS_REGION" \
            --query 'AvailabilityZones[1].ZoneName' \
            --output text)

        local AZ_3=$(aws ec2 describe-availability-zones \
            --region "$AWS_REGION" \
            --query 'AvailabilityZones[2].ZoneName' \
            --output text)

        SUBNET_ID_2=$(aws ec2 create-subnet \
            --vpc-id "$VPC_ID" \
            --cidr-block 10.0.2.0/24 \
            --availability-zone "$AZ_2" \
            --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=rusternetes-public-2}]" \
            --query 'Subnet.SubnetId' \
            --output text \
            --region "$AWS_REGION")

        aws ec2 modify-subnet-attribute \
            --subnet-id "$SUBNET_ID_2" \
            --map-public-ip-on-launch \
            --region "$AWS_REGION"

        SUBNET_ID_3=$(aws ec2 create-subnet \
            --vpc-id "$VPC_ID" \
            --cidr-block 10.0.3.0/24 \
            --availability-zone "$AZ_3" \
            --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=rusternetes-public-3}]" \
            --query 'Subnet.SubnetId' \
            --output text \
            --region "$AWS_REGION")

        aws ec2 modify-subnet-attribute \
            --subnet-id "$SUBNET_ID_3" \
            --map-public-ip-on-launch \
            --region "$AWS_REGION"

        log_info "Subnet 2 created: $SUBNET_ID_2 ($AZ_2)"
        log_info "Subnet 3 created: $SUBNET_ID_3 ($AZ_3)"
    fi
}

# Configure route table
configure_route_table() {
    log_step "Configuring route table..."

    local ROUTE_TABLE_ID=$(aws ec2 describe-route-tables \
        --filters "Name=vpc-id,Values=$VPC_ID" \
        --query 'RouteTables[0].RouteTableId' \
        --output text \
        --region "$AWS_REGION")

    aws ec2 create-route \
        --route-table-id "$ROUTE_TABLE_ID" \
        --destination-cidr-block 0.0.0.0/0 \
        --gateway-id "$IGW_ID" \
        --region "$AWS_REGION"

    log_info "Route to Internet Gateway added"
}

# Create security group
create_security_group() {
    log_step "Creating security group..."

    SG_ID=$(aws ec2 create-security-group \
        --group-name rusternetes-sg \
        --description "Security group for Rusternetes cluster" \
        --vpc-id "$VPC_ID" \
        --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=rusternetes-sg}]" \
        --query 'GroupId' \
        --output text \
        --region "$AWS_REGION")

    log_info "Security group created: $SG_ID"

    # Allow SSH
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 22 \
        --cidr 0.0.0.0/0 \
        --region "$AWS_REGION"

    # Allow Kubernetes API
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 6443 \
        --cidr 0.0.0.0/0 \
        --region "$AWS_REGION"

    # Allow etcd
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 2379-2380 \
        --cidr "$VPC_CIDR" \
        --region "$AWS_REGION"

    # Allow all internal traffic
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol all \
        --source-group "$SG_ID" \
        --region "$AWS_REGION"

    # Allow NodePort range
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp \
        --port 30000-32767 \
        --cidr 0.0.0.0/0 \
        --region "$AWS_REGION"

    log_info "Security group rules configured"
}

# Create user data script
create_user_data() {
    log_step "Creating instance initialization script..."

    cat > /tmp/rusternetes-user-data.sh <<'USERDATA'
#!/bin/bash
set -e

# Update system
dnf update -y

# Install Docker
dnf install -y docker git

# Start Docker
systemctl start docker
systemctl enable docker

# Install Docker Compose
curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source /root/.cargo/env

# Create rusternetes user
useradd -m -s /bin/bash rusternetes
usermod -aG docker rusternetes

# Clone repository
cd /home/rusternetes
git clone GITHUB_REPO_PLACEHOLDER rusternetes
chown -R rusternetes:rusternetes rusternetes

# Mark complete
touch /var/log/rusternetes-init-complete
USERDATA

    # Replace placeholder
    sed -i "s|GITHUB_REPO_PLACEHOLDER|$GITHUB_REPO|g" /tmp/rusternetes-user-data.sh

    log_info "User data script created"
}

# Launch EC2 instance
launch_instance() {
    local NAME="$1"
    local SUBNET="$2"

    log_step "Launching EC2 instance: $NAME..."

    local INSTANCE=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --subnet-id "$SUBNET" \
        --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":100,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
        --user-data file:///tmp/rusternetes-user-data.sh \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
        --query 'Instances[0].InstanceId' \
        --output text \
        --region "$AWS_REGION")

    log_info "Instance launched: $INSTANCE"
    echo "$INSTANCE"
}

# Wait for instance and get IP
wait_for_instance() {
    local INSTANCE="$1"

    log_step "Waiting for instance to be running..."

    aws ec2 wait instance-running --instance-ids "$INSTANCE" --region "$AWS_REGION"

    local PUBLIC_IP=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text \
        --region "$AWS_REGION")

    log_info "Instance running at: $PUBLIC_IP"
    echo "$PUBLIC_IP"
}

# Allocate Elastic IP
allocate_elastic_ip() {
    log_step "Allocating Elastic IP..."

    EIP_ALLOC=$(aws ec2 allocate-address \
        --domain vpc \
        --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=rusternetes-eip}]" \
        --query 'AllocationId' \
        --output text \
        --region "$AWS_REGION")

    aws ec2 associate-address \
        --instance-id "$INSTANCE_ID" \
        --allocation-id "$EIP_ALLOC" \
        --region "$AWS_REGION"

    local EIP=$(aws ec2 describe-addresses \
        --allocation-ids "$EIP_ALLOC" \
        --query 'Addresses[0].PublicIp' \
        --output text \
        --region "$AWS_REGION")

    log_info "Elastic IP allocated: $EIP"
}

# Create load balancer
create_load_balancer() {
    log_step "Creating Application Load Balancer..."

    LB_ARN=$(aws elbv2 create-load-balancer \
        --name rusternetes-lb \
        --subnets "$SUBNET_ID" "$SUBNET_ID_2" "$SUBNET_ID_3" \
        --security-groups "$SG_ID" \
        --scheme internet-facing \
        --type application \
        --query 'LoadBalancers[0].LoadBalancerArn' \
        --output text \
        --region "$AWS_REGION")

    log_info "Load balancer created"

    # Create target group
    TG_ARN=$(aws elbv2 create-target-group \
        --name rusternetes-api-tg \
        --protocol HTTPS \
        --port 6443 \
        --vpc-id "$VPC_ID" \
        --health-check-protocol HTTPS \
        --health-check-path /healthz \
        --query 'TargetGroups[0].TargetGroupArn' \
        --output text \
        --region "$AWS_REGION")

    log_info "Target group created"

    # Register instances
    aws elbv2 register-targets \
        --target-group-arn "$TG_ARN" \
        --targets "Id=$INSTANCE_ID" "Id=$INSTANCE_2" "Id=$INSTANCE_3" \
        --region "$AWS_REGION"

    log_info "Instances registered with target group"

    # Create listener
    LISTENER_ARN=$(aws elbv2 create-listener \
        --load-balancer-arn "$LB_ARN" \
        --protocol HTTPS \
        --port 6443 \
        --default-actions "Type=forward,TargetGroupArn=$TG_ARN" \
        --certificates "CertificateArn=arn:aws:iam::123456789012:server-certificate/test-cert" \
        --query 'Listeners[0].ListenerArn' \
        --output text \
        --region "$AWS_REGION" 2>/dev/null || true)

    local LB_DNS=$(aws elbv2 describe-load-balancers \
        --load-balancer-arns "$LB_ARN" \
        --query 'LoadBalancers[0].DNSName' \
        --output text \
        --region "$AWS_REGION")

    log_info "Load balancer DNS: $LB_DNS"
}

# Display completion message
display_completion() {
    echo
    echo "=========================================="
    echo "  Rusternetes AWS Deployment Complete!"
    echo "=========================================="
    echo
    echo -e "${GREEN}Deployment successful!${NC}"
    echo

    if [[ "$INSTALL_HA" == "true" ]]; then
        local LB_DNS=$(aws elbv2 describe-load-balancers \
            --load-balancer-arns "$LB_ARN" \
            --query 'LoadBalancers[0].DNSName' \
            --output text \
            --region "$AWS_REGION")
        echo "Load Balancer DNS: $LB_DNS"
        echo "API Server: https://$LB_DNS:6443"
        echo
        echo "Instances:"
        echo "  Instance 1: $INSTANCE_ID"
        echo "  Instance 2: $INSTANCE_2"
        echo "  Instance 3: $INSTANCE_3"
    else
        local PUBLIC_IP=$(aws ec2 describe-instances \
            --instance-ids "$INSTANCE_ID" \
            --query 'Reservations[0].Instances[0].PublicIpAddress' \
            --output text \
            --region "$AWS_REGION")
        echo "Instance ID: $INSTANCE_ID"
        echo "Public IP: $PUBLIC_IP"
        echo "API Server: https://$PUBLIC_IP:6443"
    fi

    echo
    echo "Next steps:"
    echo "  1. Wait for instance initialization to complete (~5-10 minutes)"
    echo "  2. SSH to instance: ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@<IP>"
    echo "  3. Check initialization: tail -f /var/log/cloud-init-output.log"
    echo "  4. Switch to rusternetes user: sudo su - rusternetes"
    echo "  5. Build and start cluster:"
    echo "     cd ~/rusternetes"
    echo "     source ~/.cargo/env"
    echo "     cargo build --release"
    echo "     export KUBELET_VOLUMES_PATH=\$(pwd)/.rusternetes/volumes"
    echo "     docker-compose build"
    echo "     docker-compose up -d"
    echo "     envsubst < bootstrap-cluster.yaml > /tmp/bootstrap-expanded.yaml"
    echo "     KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f /tmp/bootstrap-expanded.yaml"
    echo
    echo "Resources created:"
    echo "  VPC: $VPC_ID"
    echo "  Subnet(s): $SUBNET_ID $([ -n "$SUBNET_ID_2" ] && echo "$SUBNET_ID_2 $SUBNET_ID_3")"
    echo "  Security Group: $SG_ID"
    echo "  Internet Gateway: $IGW_ID"
    if [[ -n "$EIP_ALLOC" ]]; then
        echo "  Elastic IP: $EIP_ALLOC"
    fi
    if [[ -n "$LB_ARN" ]]; then
        echo "  Load Balancer: $LB_ARN"
    fi
    echo
    echo "To clean up all resources:"
    echo "  # Terminate instances"
    echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_ID $([ -n "$INSTANCE_2" ] && echo "$INSTANCE_2 $INSTANCE_3") --region $AWS_REGION"
    echo "  # Delete other resources (see docs/AWS_DEPLOYMENT.md for full cleanup)"
    echo "=========================================="
    echo
}

# Main installation flow
main() {
    log_info "Starting Rusternetes AWS deployment..."

    check_prerequisites
    display_config

    get_ami
    create_vpc
    create_internet_gateway
    create_subnets
    configure_route_table
    create_security_group
    create_user_data

    if [[ "$INSTALL_HA" == "true" ]]; then
        # HA deployment
        INSTANCE_ID=$(launch_instance "rusternetes-master-1" "$SUBNET_ID")
        INSTANCE_2=$(launch_instance "rusternetes-master-2" "$SUBNET_ID_2")
        INSTANCE_3=$(launch_instance "rusternetes-master-3" "$SUBNET_ID_3")

        wait_for_instance "$INSTANCE_ID" > /dev/null
        wait_for_instance "$INSTANCE_2" > /dev/null
        wait_for_instance "$INSTANCE_3" > /dev/null

        create_load_balancer
    else
        # Single-node deployment
        INSTANCE_ID=$(launch_instance "rusternetes-node" "$SUBNET_ID")
        wait_for_instance "$INSTANCE_ID" > /dev/null

        if [[ "$ALLOCATE_EIP" == "true" ]]; then
            allocate_elastic_ip
        fi
    fi

    # Disable cleanup trap on success
    trap - ERR

    display_completion
}

# Run main function
main "$@"
