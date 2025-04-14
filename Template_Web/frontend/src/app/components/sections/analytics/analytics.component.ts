import { Component, OnInit } from '@angular/core';
import { FraudDetectionService } from '../../../services/fraud_detection.service';

interface StakeholderType {
  key: string;
  label: string;
}

interface Entity {
  id: string;
  name: string;
  type: string;
}

interface AnalysisResult {
  entityId: string;
  entityName: string;
  entityType: string;
  fraudProbability: number;
  riskTier: string;
  isFraud: boolean;
  experienceYears: number;
  entityDetails: any; // This will hold all fields from the dataset
}

interface EntityTerms {
  term: string;
  clients: string;
  transactions: string;
}

@Component({
  selector: 'app-analytics',
  templateUrl: './analytics.component.html',
  styleUrls: ['./analytics.component.css']
})
export class AnalyticsComponent implements OnInit {
  // Stakeholder types
  showAllEntityDetails: boolean = false;
  stakeholderTypes: StakeholderType[] = [
    { key: 'players_agents', label: 'Players Agents' },
    { key: 'recruiting_agents', label: 'Recruiting Agents' },
    { key: 'communication_boxes', label: 'Communication Boxes' },
    { key: 'sporting_management_agencies', label: 'Sporting Management Agencies' },
    { key: 'sponsors', label: 'Sponsors' }
  ];

  // Entity terms for each stakeholder type
  entityTerms: { [key: string]: EntityTerms } = {
    'players_agents': { term: 'agent', clients: 'players', transactions: 'contracts' },
    'recruiting_agents': { term: 'recruiter', clients: 'athletes', transactions: 'recruitment deals' },
    'sporting_management_agencies': { term: 'agency', clients: 'teams/athletes', transactions: 'management contracts' },
    'communication_boxes': { term: 'communication box', clients: 'media outlets', transactions: 'press releases' },
    'sponsors': { term: 'sponsor', clients: 'sponsored entities', transactions: 'sponsorship deals' }
  };

  // Component state variables
  selectedStakeholderType: string = '';
  selectedEntityId: string = '';
  entities: Entity[] = [];
  analysisResult: AnalysisResult | null = null;
  loading: boolean = false;

  constructor(private fraudDetectionService: FraudDetectionService) { }

  ngOnInit(): void {
    // Nothing to initialize on component load
  }

  // Inside onStakeholderTypeChange() method:
onStakeholderTypeChange(): void {
  console.log(`Stakeholder type changed to: ${this.selectedStakeholderType}`);
  
  // Reset entity selection when stakeholder type changes
  this.selectedEntityId = '';
  this.analysisResult = null;
  
  if (!this.selectedStakeholderType) {
    this.entities = [];
    return;
  }
  
  this.loading = true;
  
  // Load entities for the selected stakeholder type
  this.fraudDetectionService.getEntitiesByType(this.selectedStakeholderType)
    .subscribe({
      next: (data) => {
        console.log(`Received ${data.length} entities:`, data);
        this.entities = data;
        this.loading = false;
      },
      error: (err) => {
        console.error('Error loading entities:', err);
        this.loading = false;
      }
    });
}

viewCaseStudy(): void {
  if (!this.selectedEntityId) return;
  
  this.loading = true;
  this.analysisResult = null;
  
  console.log(`Sending request for entity ID: ${this.selectedEntityId}, type: ${this.selectedStakeholderType}`);
  
  // Call the fraud detection service to analyze the selected entity
  this.fraudDetectionService.detectFraud({
    entityId: this.selectedEntityId,
    entityType: this.selectedStakeholderType
  }).subscribe({
    next: (result) => {
      console.log('Received result:', result);  // Debug logging
      
      // Find the selected entity
      const entity = this.entities.find(e => e.id === this.selectedEntityId);
      
      if (!entity) {
        console.error('Selected entity not found in entities list');
        this.loading = false;
        return;
      }
      
      if (!result.entityDetails) {
        console.error('No entity details returned from API');
        this.loading = false;
        return;
      }
      
      // Create the analysis result using actual data from CSV
      this.analysisResult = {
        entityId: this.selectedEntityId,
        entityName: entity.name,
        entityType: this.selectedStakeholderType,
        fraudProbability: result.fraudProbability,
        riskTier: result.riskTier,
        isFraud: result.isFraud,
        experienceYears: result.entityDetails.years_in_business || 
                        result.entityDetails.experience_years || 
                        result.entityDetails.years_experience || 0,
        entityDetails: result.entityDetails
      };
      
      // Scroll down to the results section
      setTimeout(() => {
        const resultsElement = document.querySelector('.case-study-results');
        if (resultsElement) {
          resultsElement.scrollIntoView({ behavior: 'smooth' });
        }
      }, 100);
      
      this.loading = false;
    },
    error: (err) => {
      console.error('Error analyzing entity:', err);
      // Display the error in the UI instead of silently failing
      alert(`Error: ${err.error?.error || 'Could not retrieve data from server'}`);
      this.loading = false;
    }
  });
}

toggleEntityDetails(): void {
  this.showAllEntityDetails = !this.showAllEntityDetails;
} 

getKeyEntityFields(): string[] {
  // Return key fields to show in the compact view based on entity type
  if (!this.analysisResult || !this.analysisResult.entityDetails) return [];
  
  const commonFields = ['license_number'];
  
  // Add stakeholder-specific key fields
  switch (this.selectedStakeholderType) {
    case 'players_agents':
      return [...commonFields, 'specialization', 'avg_contract_value'];
    case 'recruiting_agents':
      return [...commonFields, 'recruitment_success_rate', 'athlete_placement_count', 'primary_sports'];
    case 'sporting_management_agencies':
      return [...commonFields, 'num_represented_entities', 'annual_revenue', 'founded_year'];
    case 'communication_boxes':
      return [...commonFields, 'media_reach', 'platform_count', 'primary_market'];
    case 'sponsors':
      return [...commonFields, 'sponsorship_budget', 'industry_sector', 'sponsored_entities_count'];
    default:
      return commonFields;
  }
}

getEntityDetailsFields(): string[] {
  // Return all available fields from the entity details
  if (!this.analysisResult || !this.analysisResult.entityDetails) return [];
  return Object.keys(this.analysisResult.entityDetails).sort();
}

formatFieldName(fieldName: string): string {
  // Convert snake_case to Title Case
  return fieldName
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

formatFieldValue(field: string, value: any): string {
  // Format values based on field type
  if (value === null || value === undefined) {
    return 'N/A';
  }
  
  // Format dates
  if (field.includes('date')) {
    const date = new Date(value);
    if (!isNaN(date.getTime())) {
      return date.toLocaleDateString();
    }
  }
  
  // Format percentages
  if (field.includes('rate') || field.includes('percentage') || field.includes('ratio')) {
    const num = parseFloat(value);
    if (!isNaN(num)) {
      return num.toFixed(1) + '%';
    }
  }
  
  // Format currency
  if (field.includes('price') || field.includes('value') || field.includes('fee') || 
      field.includes('budget') || field.includes('revenue')) {
    const num = parseFloat(value);
    if (!isNaN(num)) {
      return '$' + num.toLocaleString();
    }
  }
  
  // Return as is for other types
  return value;
}

  // Helper methods for the template
  getStakeholderLabel(): string {
    const stakeholder = this.stakeholderTypes.find(s => s.key === this.selectedStakeholderType);
    return stakeholder ? stakeholder.label : '';
  }

  getEntityTerm(): string {
    if (!this.selectedStakeholderType) return 'entity';
    return this.entityTerms[this.selectedStakeholderType]?.term || 'entity';
  }

  getFraudColorClass(): string {
    if (!this.analysisResult) return '';
    
    if (this.analysisResult.fraudProbability > 70) return 'text-danger';
    if (this.analysisResult.fraudProbability > 40) return 'text-warning';
    return 'text-success';
  }

  getRiskTierClass(): string {
    if (!this.analysisResult) return '';
    
    switch (this.analysisResult.riskTier) {
      case 'VERY HIGH': return 'risk-very-high';
      case 'HIGH': return 'risk-high';
      case 'MEDIUM': return 'risk-medium';
      case 'LOW': return 'risk-low';
      case 'VERY LOW': return 'risk-very-low';
      default: return '';
    }
  }

  getGaugeRotation(): number {
    if (!this.analysisResult) return 0;
    // Convert percentage to rotation degrees (0-180)
    return (this.analysisResult.fraudProbability / 100) * 180;
  }

  // Risk factors generation - similar to your Python code
  getHighRiskFactors(result: AnalysisResult): string[] {
    if (!result) return [];
    
    const probability = result.fraudProbability / 100;
    const stakeholderType = result.entityType;
    
    // Define stakeholder-specific risk factors (following your Python implementation)
    if (stakeholderType === 'players_agents') {
      return [
        `Acquired ${Math.round(probability * 200)} new players in less than a year (highly unusual growth rate)`,
        `Maintained ${Math.round(probability * 15)} offshore accounts with irregular transaction patterns`,
        "Documentation inconsistencies in 62% of player contracts",
        `Connected to ${Math.round(probability * 8)} previously sanctioned agencies`,
        "Multiple identity discrepancies across official documents"
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    } 
    else if (stakeholderType === 'recruiting_agents') {
      return [
        `Claimed ${Math.round(probability * 150)} athlete placements with no verification`,
        `Maintained ${Math.round(probability * 12)} unregistered scouting operations`,
        `Collected ${Math.round(probability * 50)}% higher fees than industry standard from athletes`,
        "Multiple recruitment claims proved falsified upon investigation",
        `Connected to ${Math.round(probability * 7)} training centers with poor safety records`
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    }
    else if (stakeholderType === 'sporting_management_agencies') {
      return [
        `Managed ${Math.round(probability * 300)}% growth in client portfolio with minimal staff increase`,
        `Operated ${Math.round(probability * 10)} shell companies tied to player management`,
        "Significant discrepancies in reported vs. actual revenue",
        `Failed ${Math.round(probability * 80)}% of regulatory compliance checks`,
        "Pattern of contract disputes with multiple clubs and players"
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    }
    else if (stakeholderType === 'communication_boxes') {
      return [
        `Published ${Math.round(probability * 120)} articles with unverifiable sources`,
        `Created ${Math.round(probability * 15)} false media outlets to amplify stories`,
        `Coordinated ${Math.round(probability * 40)} narrative campaigns with betting pattern spikes`,
        "Significant discrepancies between reported news and verifiable facts",
        `Connected to ${Math.round(probability * 8)} previously sanctioned information brokers`
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    }
    else if (stakeholderType === 'sponsors') {
      return [
        `Created ${Math.round(probability * 20)} shell sponsorship deals to launder $${Math.round(probability * 5000000)}`,
        `Reported ${Math.round(probability * 250)}% ROI on sponsorships with no supporting data`,
        "Significant discrepancies between contracted and actual payments",
        `Connected to ${Math.round(probability * 12)} sanctioned financial entities`,
        "Pattern of sponsorship deals with subsequent undisclosed related-party transactions"
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    }
    else {
      // Generic risk factors for other types
      return [
        `Reported ${Math.round(probability * 250)}% growth with minimal substantiation`,
        `Maintained ${Math.round(probability * 15)} suspicious financial arrangements`,
        `Failed ${Math.round(probability * 80)}% of compliance checks`,
        "Multiple identity and documentation discrepancies",
        `Connected to ${Math.round(probability * 8)} previously sanctioned entities`
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    }
  }

  getMediumRiskFactors(result: AnalysisResult): string[] {
    if (!result) return [];
    
    const probability = result.fraudProbability / 100;
    const stakeholderType = result.entityType;
    
    // Define stakeholder-specific medium risk factors
    if (stakeholderType === 'players_agents') {
      return [
        `Unusual growth of ${Math.round(probability * 100)} players in the past year`,
        "Some inconsistencies in contract documentation",
        `${Math.round(probability * 30)}% of financial transactions require additional verification`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    } 
    else if (stakeholderType === 'recruiting_agents') {
      return [
        `Unusual recruitment success rate of ${Math.round(probability * 90)}% (industry average is 30%)`,
        "Some inconsistencies in athlete placement records",
        `${Math.round(probability * 25)}% of claimed placements couldn't be verified`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    }
    else if (stakeholderType === 'sporting_management_agencies') {
      return [
        `Growth rate of ${Math.round(probability * 80)}% exceeds typical agency expansion`,
        "Some client contracts contain unusual exclusivity clauses",
        `${Math.round(probability * 20)}% of financial transactions show timing anomalies`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    }
    else if (stakeholderType === 'communication_boxes') {
      return [
        `Published ${Math.round(probability * 60)} stories with single anonymous sources`,
        "Some published content contains factual inconsistencies",
        `${Math.round(probability * 30)}% of exclusives couldn't be corroborated by other outlets`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    }
    else if (stakeholderType === 'sponsors') {
      return [
        `Sponsorship reporting shows ${Math.round(probability * 70)}% discrepancies with third-party audits`,
        "Some promotional activities lack verification documentation",
        `${Math.round(probability * 25)}% of claimed audience reach appears inflated`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    }
    else {
      // Generic medium risk factors
      return [
        `Growth rate of ${Math.round(probability * 100)}% exceeds industry averages`,
        "Some documentation contains inconsistencies",
        `${Math.round(probability * 30)}% of transactions require additional verification`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    }
  }

  getLowRiskReasons(): string[] {
    return [
      "Consistent documentation across all relationships and transactions",
      "Stable growth rate within industry norms",
      "Clean compliance history with no significant issues",
      "All financial transactions properly documented and verified",
      "Business practices align with industry standards",
      "Successfully passed independent audits"
    ].slice(0, 3);
  }


  // Add these methods to your component class

/**
 * Handle card selection and trigger entity loading
 */
selectStakeholderType(typeKey: string): void {
  // Only make API call if selection changed
  if (this.selectedStakeholderType !== typeKey) {
    this.selectedStakeholderType = typeKey;
    this.selectedEntityId = ''; // Clear selected entity when changing type
    this.onStakeholderTypeChange(); // This will load entities for the selected type
  }
}

/**
 * Get description text for each stakeholder type
 */
getStakeholderDescription(typeKey: string): string {
  switch (typeKey) {
    case 'players_agents':
      return 'Professionals representing athletes in contract negotiations and career management.';
    case 'recruiting_agents':
      return 'Specialists in identifying talent and facilitating athlete placement in teams.';
    case 'sporting_management_agencies':
      return 'Companies providing comprehensive representation for athletes and sports entities.';
    case 'communication_boxes':
      return 'Media entities handling public relations and messaging for sports figures.';
    case 'sponsors':
      return 'Companies investing in sports through endorsements and sponsorship deals.';
    default:
      return 'Organizations involved in sports business operations.';
  }
}
  
}