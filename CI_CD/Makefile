# Terraform initialization
init:
	@echo "Initializing Terraform..."
	terraform init

# Terraform plan creation
plan:
	@echo "Generating Terraform plan..."
	terraform plan -var-file=terraform.tfvars -lock=false

# Terraform apply
apply:
	@echo "Applying Terraform changes..."
	terraform apply -var-file=terraform.tfvars -lock=false -auto-approve

output job_ids:
	@echo "the job IDs are..."
	terraform output job_ids

# Terraform deletion
destroy:
	@echo "Destroying Terraform infrastructure..."
	terraform destroy -auto-approve





