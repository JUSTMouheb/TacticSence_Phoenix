import { Component, OnInit, AfterViewInit, DoCheck } from '@angular/core';
import { FraudDetectionService } from '../../../services/fraud_detection.service';
import { CsvService } from '../../../services/CsvService';
interface StakeholderType {
  key: string;
  label: string;
}

interface Entity {
  id: string;
  name: string;
  type: string;
  verification_status?: string;
  verification_date?: string;
  email?: string;
  phone?: string;
  location?: string;
  experience_years?: number;
  years_experience?: number;
  specialization?: string;
  languages?: string;
  certification?: string;
}

interface AnalysisResult {
  entityId: string;
  entityName: string;
  entityType: string;
  fraudProbability: number;
  riskTier: string;
  isFraud: boolean;
  experienceYears: number;
  entityDetails: any;
}

interface EntityTerms {
  term: string;
  clients: string;
  transactions: string;
}

interface PaginationState {
  currentPage: number;
  totalPages: number;
}

@Component({
  selector: 'app-analytics',
  templateUrl: './analytics.component.html',
  styleUrls: ['./analytics.component.css']
})
export class AnalyticsComponent implements OnInit, AfterViewInit, DoCheck {
  verifiedEntities: Entity[] = [];
  selectedEntity: any = null;
  showAllDetails: boolean = false;
  
  entityImageMap: Map<string, string> = new Map();

  // Soccer images with correct path
footballImages: string[] = [
    'assets/img/soccer/soccer1.avif',
    'assets/img/soccer/soccer2.avif',
    'assets/img/soccer/soccer3.avif',
    'assets/img/soccer/soccer4.avif',
    'assets/img/soccer/soccer5.avif',
    'assets/img/soccer/soccer6.avif',
    'assets/img/soccer/soccer7.avif',
    'assets/img/soccer/soccer8.avif',
    'assets/img/soccer/soccer9.avif',
    'assets/img/soccer/soccer10.avif',
    'assets/img/soccer/soccer11.avif',
    'assets/img/soccer/soccer12.avif'
];
 recruitingAgentImages: string[] = [
    'assets/img/recagents/recagent1.avif',
    'assets/img/recagents/recagent2.avif',
    'assets/img/recagents/recagent3.avif',
    'assets/img/recagents/recagent4.avif',
    'assets/img/recagents/recagent5.avif',
    'assets/img/recagents/recagent6.avif',
    'assets/img/recagents/recagent7.avif',
    'assets/img/recagents/recagent8.avif',
    'assets/img/recagents/recagent9.avif',
    'assets/img/recagents/recagent10.avif',
    'assets/img/recagents/recagent11.avif',
    'assets/img/recagents/recagent12.avif'
];

communicationBoxImages: string[] = [
    'assets/img/combox/combox1.avif',
    'assets/img/combox/combox2.avif',
    'assets/img/combox/combox3.avif',
    'assets/img/combox/combox4.avif',
    'assets/img/combox/combox5.avif',
    'assets/img/combox/combox6.avif',
    'assets/img/combox/combox7.avif',
    'assets/img/combox/combox8.avif',
    'assets/img/combox/combox9.avif',
    'assets/img/combox/combox10.avif',
    'assets/img/combox/combox11.avif',
    'assets/img/combox/combox12.avif'
];

// Sporting Management Agencies images with correct path
sportingManagementImages: string[] = [
  'assets/img/sma/sma1.avif',
  'assets/img/sma/sma2.avif',
  'assets/img/sma/sma3.avif',
  'assets/img/sma/sma4.avif',
  'assets/img/sma/sma5.avif',
  'assets/img/sma/sma6.avif',
  'assets/img/sma/sma7.avif',
  'assets/img/sma/sma8.avif',
  'assets/img/sma/sma9.avif',
  'assets/img/sma/sma10.avif',
  'assets/img/sma/sma11.avif',
  'assets/img/sma/sma12.avif'
];
  itemsPerPage: number = 12;
  paginationState: { [key: string]: PaginationState } = {
    'all': { currentPage: 1, totalPages: 1 },
    'players_agents': { currentPage: 1, totalPages: 1 },
    'recruiting_agents': { currentPage: 1, totalPages: 1 },
    'communication_boxes': { currentPage: 1, totalPages: 1 },
    'sporting_management_agencies': { currentPage: 1, totalPages: 1 },
    'sponsors': { currentPage: 1, totalPages: 1 }
  };
  Math = Math;

  showAllEntityDetails: boolean = false;
  stakeholderTypes: StakeholderType[] = [
    { key: 'players_agents', label: 'Players Agents' },
    { key: 'recruiting_agents', label: 'Recruiting Agents' },
    { key: 'communication_boxes', label: 'Communication Boxes' },
    { key: 'sporting_management_agencies', label: 'Sporting Management Agencies' },
    { key: 'sponsors', label: 'Sponsors' }
  ];

  entityTerms: { [key: string]: EntityTerms } = {
    'players_agents': { term: 'agent', clients: 'players', transactions: 'contracts' },
    'recruiting_agents': { term: 'recruiter', clients: 'athletes', transactions: 'recruitment deals' },
    'sporting_management_agencies': { term: 'agency', clients: 'teams/athletes', transactions: 'management contracts' },
    'communication_boxes': { term: 'communication box', clients: 'media outlets', transactions: 'press releases' },
    'sponsors': { term: 'sponsor', clients: 'sponsored entities', transactions: 'sponsorship deals' }
  };

  entityCountsByType: { [key: string]: number } = {
    'all': 0,
    'players_agents': 0,
    'recruiting_agents': 0,
    'communication_boxes': 0,
    'sporting_management_agencies': 0,
    'sponsors': 0
  };

  selectedStakeholderType: string = '';
  selectedEntityId: string = '';
  entities: Entity[] = [];
  analysisResult: AnalysisResult | null = null;
  loading: boolean = false;

  // CSS class backgrounds for non-player-agent stakeholders
  private otherBackgrounds: { [key: string]: string } = {
    'recruiting_agents': 'bg-recruiting_agents',
    'communication_boxes': 'bg-communication_boxes',
    'sporting_management_agencies': 'bg-sporting_management_agencies',
    'sponsors': 'bg-sponsors'
  };

  constructor(
    private fraudDetectionService: FraudDetectionService,
    private csvService: CsvService
  ) {
    this.selectedStakeholderType = this.stakeholderTypes.length > 0 ? this.stakeholderTypes[0].key : '';
  }
  
  
  ngOnInit(): void {
    this.onStakeholderTypeChange();
    
    // Call the image check method
    this.checkImagePaths();
  }
  
  // Define the method at class level, not inside another method
  checkImagePaths(): void {
    this.footballImages.forEach((path, index) => {
      console.log(`Checking image ${index+1}: ${path}`);
      fetch(path)
        .then(response => {
          if (response.ok) {
            console.log(`✅ Image exists: ${path}`);
          } else {
            console.error(`❌ Image NOT found (${response.status}): ${path}`);
          }
        })
        .catch(error => console.error(`❌ Error fetching image: ${path}`, error));
    });
  }
  
  // Get random image from football images
  getRandomImage(): string {
    const index = Math.floor(Math.random() * this.footballImages.length);
    return this.footballImages[index];
  }

  // Handle image errors with generic fallback
  handleImageError(event: any): void {
    const imgElement = event.target;
    if (imgElement) {
      imgElement.classList.add('img-error');
    }
  }

  ngDoCheck() {
    this.updatePaginationState();
  }

  updatePaginationState(): void {
    this.paginationState['all'].totalPages = Math.ceil(this.verifiedEntities.length / this.itemsPerPage);
    this.entityCountsByType['all'] = this.verifiedEntities.length;

    this.stakeholderTypes.forEach(type => {
      const count = this.verifiedEntities.filter(entity => entity.type === type.key).length;
      this.paginationState[type.key].totalPages = Math.ceil(count / this.itemsPerPage);
      this.entityCountsByType[type.key] = count;
      if (this.paginationState[type.key].currentPage > this.paginationState[type.key].totalPages && 
          this.paginationState[type.key].totalPages > 0) {
        this.paginationState[type.key].currentPage = 1;
      }
    });
  }
  
  ngAfterViewInit(): void {
    this.initIsotope();
    // Make sure backgrounds are assigned after view init
    this.assignBackgroundClasses();
    
    // Force apply backgrounds to DOM elements after a delay
    setTimeout(() => {
      this.applyBackgroundsToDom();
    }, 500);
  }
  
// New method to directly apply backgrounds to DOM elements
applyBackgroundsToDom(): void {
  this.verifiedEntities.forEach(entity => {
    const imagePath = this.entityImageMap.get(entity.id);
    if (imagePath) {
      const elements = document.querySelectorAll(`[data-entity-id="${entity.id}"] .portfolio-img`);
      elements.forEach(el => {
        // Apply background images for all stakeholder types that have images
        if (entity.type === 'players_agents' || 
            entity.type === 'recruiting_agents' || 
            entity.type === 'communication_boxes' || 
            entity.type === 'sporting_management_agencies' || 
            entity.type === 'sponsors') {
          (el as HTMLElement).style.backgroundImage = `url(${imagePath})`;
          (el as HTMLElement).style.backgroundSize = 'cover';
          (el as HTMLElement).style.backgroundPosition = 'center';
          console.log(`Applied background to DOM for ${entity.name} (${entity.type}): ${imagePath}`);
        }
      });
    }
  });
}

  initIsotope(): void {
    setTimeout(() => {
      const isotopeContainer = document.querySelector('.isotope-container');
      const isotopeFilters = document.querySelectorAll('.isotope-filters li');

      if (isotopeContainer) {
        const iso = new (window as any).Isotope(isotopeContainer, {
          itemSelector: '.isotope-item',
          layoutMode: 'masonry'
        });

        if (isotopeFilters) {
          isotopeFilters.forEach(filter => {
            filter.addEventListener('click', function(this: HTMLElement) {
              isotopeFilters.forEach(f => f.classList.remove('filter-active'));
              this.classList.add('filter-active');
              const filterValue = this.getAttribute('data-filter') || '.filter-players_agents';
              const typeKey = filterValue.replace('.filter-', '');
              this.ownerDocument.defaultView?.dispatchEvent(new CustomEvent('stakeholderTypeSelected', {
                detail: { type: typeKey }
              }));
              iso.arrange({ filter: filterValue });
            });
          });
        }

        window.addEventListener('stakeholderTypeSelected', (event: any) => {
          this.selectStakeholderType(event.detail.type);
        });

        if ((window as any).GLightbox) {
          const lightbox = (window as any).GLightbox({
            selector: '.glightbox'
          });
        }
      }
    }, 500);
  }

  onStakeholderTypeChange(): void {
    console.log(`Stakeholder type changed to: ${this.selectedStakeholderType}`);
    this.selectedEntityId = '';
    this.selectedEntity = null;
    this.analysisResult = null;

    if (!this.selectedStakeholderType) return;

    this.loading = true;
    this.fraudDetectionService.getVerifiedEntitiesByType(this.selectedStakeholderType)
      .subscribe({
        next: (data) => {
          const otherEntities = this.verifiedEntities.filter(
            entity => entity.type !== this.selectedStakeholderType
          );
          this.verifiedEntities = [...otherEntities, ...data];

          // Assign background classes for entities
          this.assignBackgroundClasses();
          
          // Apply backgrounds directly after data loads
          setTimeout(() => {
            this.applyBackgroundsToDom();
          }, 100);
          
          this.loading = false;
        },
        error: (err) => {
          console.error('Error loading verified entities:', err);
          this.loading = false;
        }
      });
  }

  // Update assignBackgroundClasses to handle all stakeholder types
private assignBackgroundClasses(): void {
  this.entityImageMap.clear();
  
  // Create shuffled copies of all image arrays
  const shuffledImages = {
    'players_agents': [...this.footballImages].sort(() => Math.random() - 0.5),
    'recruiting_agents': [...this.recruitingAgentImages].sort(() => Math.random() - 0.5),
    'communication_boxes': [...this.communicationBoxImages].sort(() => Math.random() - 0.5),
    'sporting_management_agencies': [...this.sportingManagementImages].sort(() => Math.random() - 0.5),
    'sponsors': [...this.footballImages].sort(() => Math.random() - 0.5), // Add this line to use football images for sponsors
  
  };
  
  // Initialize counters for each type
  const typeCounters = {
    'players_agents': 0,
    'recruiting_agents': 0,
    'communication_boxes': 0,
    'sporting_management_agencies': 0,
    'sponsors': 0
  };
  
  // Process all entities
  this.verifiedEntities.forEach(entity => {
    if (shuffledImages[entity.type]) {
      const imgIndex = typeCounters[entity.type] % shuffledImages[entity.type].length;
      const imagePath = shuffledImages[entity.type][imgIndex];
      
      // Verify the image exists before assigning
      fetch(imagePath)
        .then(response => {
          if (response.ok) {
            this.entityImageMap.set(entity.id, imagePath);
            console.log(`${entity.type} ${entity.name} (${entity.id}) assigned image: ${imagePath}`);
          } else {
            // Fallback to a default image if the assigned one doesn't exist
            const fallbackImage = `../../assets/img/combox/combox1.avif`;
            this.entityImageMap.set(entity.id, fallbackImage);
            console.warn(`Image not found: ${imagePath}, using fallback: ${fallbackImage}`);
          }
        })
        .catch(error => {
          console.error(`Error checking image: ${imagePath}`, error);
          const fallbackImage = `../../assets/img/combox/combox1.avif`;
          this.entityImageMap.set(entity.id, fallbackImage);
        });
      
      typeCounters[entity.type]++;
    } else {
      // Fallback for any stakeholder type without images
      const bgClass = this.otherBackgrounds[entity.type] || `bg-${entity.type}`;
      this.entityImageMap.set(entity.id, bgClass);
    }
  });
}

 // Update getEntityBackgroundStyle to handle all types
getEntityBackgroundStyle(entityId: string): { [key: string]: string } {
  const background = this.entityImageMap.get(entityId);
  if (!background) {
    return { 
      'background-image': 'url(assets/img/soccer/soccer1.avif)', 
      'background-size': 'cover', 
      'background-position': 'center',
      'background-repeat': 'no-repeat'
    };
  }
  if (!background) {
    return { 
      'background-image': 'url(../../assets/img/combox/combox1.avif)', 
      'background-size': 'cover', 
      'background-position': 'center',
      'background-repeat': 'no-repeat'
    };
  }
  
  // If it's a path (not a CSS class that starts with 'bg-'), use it as background image
  if (!background.startsWith('bg-')) {
    return { 
      'background-image': `url(${background})`,
      'background-size': 'cover',
      'background-position': 'center',
      'background-repeat': 'no-repeat'
    };
  }
  // Otherwise return empty object (class will be applied by ngClass)
  return {};
}
  getEntityBackgroundClass(entityId: string): string {
    const background = this.entityImageMap.get(entityId);
    if (background && background.startsWith('bg-')) {
      return background;
    }
    return ''; // No class if using image URL
  }

  showEntityDetails(entity: Entity): void {
    this.loading = true;
    this.fraudDetectionService.detectFraud({
      entityId: entity.id,
      entityType: entity.type
    }).subscribe({
      next: (result) => {
        this.selectedEntity = {
          ...entity,
          ...result.entityDetails,
          verification_status: 'Verified',
          verification_date: '2025-04-16'
        };
        this.loading = false;
      },
      error: (err) => {
        console.error('Error getting entity details:', err);
        this.selectedEntity = {
          ...entity,
          error: 'Could not load complete details'
        };
        this.loading = false;
      }
    });
  }

  closeEntityDetails(): void {
    this.selectedEntity = null;
  }

  toggleAllDetails(): void {
    this.showAllDetails = !this.showAllDetails;
  }

  getEntityKeys(): string[] {
    if (!this.selectedEntity) return [];
    return Object.keys(this.selectedEntity).sort();
  }

  selectStakeholderType(typeKey: string): void {
    if (this.selectedStakeholderType !== typeKey && typeKey) {
      this.selectedStakeholderType = typeKey;
      this.selectedEntity = null;
      this.onStakeholderTypeChange();
    }
  }

  viewCaseStudy(): void {
    if (!this.selectedEntityId) return;

    this.loading = true;
    this.analysisResult = null;

    this.fraudDetectionService.detectFraud({
      entityId: this.selectedEntityId,
      entityType: this.selectedStakeholderType
    }).subscribe({
      next: (result) => {
        const entity = this.entities.find(e => e.id === this.selectedEntityId);
        if (!entity || !result.entityDetails) {
          console.error('Entity or details not found');
          this.loading = false;
          return;
        }

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
        alert(`Error: ${err.error?.error || 'Could not retrieve data from server'}`);
        this.loading = false;
      }
    });
  }

  toggleEntityDetails(): void {
    this.showAllEntityDetails = !this.showAllEntityDetails;
  }

  getKeyEntityFields(): string[] {
    if (!this.analysisResult || !this.analysisResult.entityDetails) return [];
    const commonFields = ['license_number'];
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
    if (!this.analysisResult || !this.analysisResult.entityDetails) return [];
    return Object.keys(this.analysisResult.entityDetails).sort();
  }

  formatFieldName(fieldName: string): string {
    return fieldName
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }

  formatFieldValue(field: string, value: any): string {
    if (value === null || value === undefined) return 'N/A';
    if (field.includes('date')) {
      const date = new Date(value);
      if (!isNaN(date.getTime())) return date.toLocaleDateString();
    }
    if (field.includes('rate') || field.includes('percentage') || field.includes('ratio')) {
      const num = parseFloat(value);
      if (!isNaN(num)) return num.toFixed(1) + '%';
    }
    if (field.includes('price') || field.includes('value') || field.includes('fee') || 
        field.includes('budget') || field.includes('revenue')) {
      const num = parseFloat(value);
      if (!isNaN(num)) return '$' + num.toLocaleString();
    }
    return value.toString();
  }

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
    return (this.analysisResult.fraudProbability / 100) * 180;
  }

  getHighRiskFactors(result: AnalysisResult): string[] {
    if (!result) return [];
    const probability = result.fraudProbability / 100;
    const stakeholderType = result.entityType;

    if (stakeholderType === 'players_agents') {
      return [
        `Acquired ${Math.round(probability * 200)} new players in less than a year (highly unusual growth rate)`,
        `Maintained ${Math.round(probability * 15)} offshore accounts with irregular transaction patterns`,
        "Documentation inconsistencies in 62% of player contracts",
        `Connected to ${Math.round(probability * 8)} previously sanctioned agencies`,
        "Multiple identity discrepancies across official documents"
      ].slice(0, Math.min(5, Math.round(probability * 7)));
    }
    return [];
  }

  getMediumRiskFactors(result: AnalysisResult): string[] {
    if (!result) return [];
    const probability = result.fraudProbability / 100;
    const stakeholderType = result.entityType;

    if (stakeholderType === 'players_agents') {
      return [
        `Unusual growth of ${Math.round(probability * 100)} players in the past year`,
        "Some inconsistencies in contract documentation",
        `${Math.round(probability * 30)}% of financial transactions require additional verification`
      ].slice(0, Math.min(3, Math.round(probability * 5)));
    }
    return [];
  }

  getLowRiskReasons(): string[] {
    return [
      "Consistent documentation across all relationships and transactions",
      "Stable growth rate within industry norms",
      "Clean compliance history with no significant issues"
    ].slice(0, 3);
  }

// Update this method to navigate to the appropriate static HTML page
navigateToDetails(entity: any): void {
  // Store the selected entity in localStorage for the HTML page to access
  localStorage.setItem(`selected${this.capitalizeFirstLetter(entity.type)}`, JSON.stringify(entity));
  
  // Get full details for the entity before navigating
  this.loading = true;
  this.fraudDetectionService.detectFraud({
    entityId: entity.id,
    entityType: entity.type
  }).subscribe({
    next: (result) => {
      // Store full entity details in localStorage
      const fullDetails = {
        ...entity,
        ...result.entityDetails,
        verification_status: entity.verification_status || 'Verified',
        verification_date: entity.verification_date || '2025-04-16'
      };
      
      localStorage.setItem(`${entity.type}FullDetails`, JSON.stringify(fullDetails));
      
      // Navigate to the static HTML page with the ID as a parameter
      const detailPage = this.getDetailPageForEntityType(entity.type);
      window.location.href = `/assets/pages/${detailPage}?id=${entity.id}`;
      this.loading = false;
    },
    error: (err) => {
      console.error(`Error loading ${entity.type} details:`, err);
      
      // Navigate even if we couldn't get full details
      const detailPage = this.getDetailPageForEntityType(entity.type);
      window.location.href = `/assets/pages/${detailPage}?id=${entity.id}`;
      this.loading = false;
    }
  });
}

// Helper method to get the correct detail page for each entity type
private getDetailPageForEntityType(entityType: string): string {
  const pageMap: {[key: string]: string} = {
    'players_agents': 'player_agent-details.html',
    'recruiting_agents': 'recruiting_agent-details.html',
    'communication_boxes': 'communication_box-details.html',
    'sporting_management_agencies': 'sporting_management-details.html',
    'sponsors': 'sponsor-details.html'
  };
  
  return pageMap[entityType] || 'player_agent-details.html'; // Default fallback
}

// Helper method to capitalize entity type for localStorage key
private capitalizeFirstLetter(text: string): string {
  return text
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join('');
}




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

  get currentPaginationState(): PaginationState {
    return this.paginationState[this.selectedStakeholderType];
  }

  get filteredEntities(): Entity[] {
    return this.verifiedEntities.filter(entity => entity.type === this.selectedStakeholderType);
  }

  get paginatedEntities(): Entity[] {
    const currentPage = this.paginationState[this.selectedStakeholderType].currentPage;
    const startIndex = (currentPage - 1) * this.itemsPerPage;
    const endIndex = startIndex + this.itemsPerPage;
    return this.filteredEntities.slice(startIndex, endIndex);
  }

  // Call after pagination changes
  goToPage(page: number): void {
    if (page >= 1 && page <= this.paginationState[this.selectedStakeholderType].totalPages) {
      this.paginationState[this.selectedStakeholderType].currentPage = page;
      this.reinitializeIsotope();
      // Re-assign backgrounds after pagination
      this.assignBackgroundClasses();
      
      // Apply backgrounds directly after pagination
      setTimeout(() => {
        this.applyBackgroundsToDom();
      }, 100);
    }
  }

  // Also add to nextPage and prevPage methods
  nextPage(): void {
    const key = this.selectedStakeholderType || 'all';
    if (this.paginationState[key].currentPage < this.paginationState[key].totalPages) {
      this.paginationState[key].currentPage++;
      this.reinitializeIsotope();
      this.assignBackgroundClasses();
      
      // Apply backgrounds directly after pagination
      setTimeout(() => {
        this.applyBackgroundsToDom();
      }, 100);
    }
  }

  prevPage(): void {
    const key = this.selectedStakeholderType || 'all';
    if (this.paginationState[key].currentPage > 1) {
      this.paginationState[key].currentPage--;
      this.reinitializeIsotope();
      this.assignBackgroundClasses();
      
      // Apply backgrounds directly after pagination
      setTimeout(() => {
        this.applyBackgroundsToDom();
      }, 100);
    }
  }

  get pageNumbers(): number[] {
    const pages: number[] = [];
    const maxPagesToShow = 5;
    const key = this.selectedStakeholderType || 'all';
    const currentPage = this.paginationState[key].currentPage;
    const totalPages = this.paginationState[key].totalPages;

    let startPage = Math.max(1, currentPage - Math.floor(maxPagesToShow / 2));
    let endPage = startPage + maxPagesToShow - 1;

    if (endPage > totalPages) {
      endPage = totalPages;
      startPage = Math.max(1, endPage - maxPagesToShow + 1);
    }

    for (let i = startPage; i <= endPage; i++) {
      pages.push(i);
    }

    return pages;
  }

  reinitializeIsotope(): void {
    setTimeout(() => {
      const isotopeContainer = document.querySelector('.isotope-container');
      if (isotopeContainer && (window as any).Isotope) {
        const iso = new (window as any).Isotope(isotopeContainer, {
          itemSelector: '.isotope-item',
          layoutMode: 'masonry'
        });
        iso.arrange();
      }
    }, 100);
  }
}