import { Component, OnInit, HostListener, AfterViewInit } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { HttpClient } from '@angular/common/http';

// Declare libraries needed for the header
declare var bootstrap: any;

interface Player {
  player_id: number;
  full_name: string;
  position: string;
  age: number;
  nationality: string;
  club: string;
  medical_history: string;
  fitness_level: number;
}

interface MedicalRecord {
  lastExamDate: string;
  doctorName: string;
  notes: string;
  vitalSigns: {
    bloodPressure: string;
    heartRate: number;
    oxygenLevel: number;
  };
  medicalTests: {
    ecg: string;
    bloodwork: string;
    imaging: string[];
  };
}

interface RecoveryPlan {
  phase: string;
  startDate: string;
  estimatedEndDate: string;
  exercises: {
    name: string;
    frequency: string;
    intensity: string;
    progress: number;
  }[];
  physioSessions: {
    frequency: string;
    focusAreas: string[];
  };
  dietRecommendations: string[];
}

interface InjuryPrediction {
  riskLevel: string;
  riskScore: number;
  vulnerableAreas: string[];
  preventativeMeasures: string[];
  recommendedWorkload: string;
}

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit, AfterViewInit {
  players: Player[] = [];
  clubs: any[] = [];
  agents: any[] = [];
  sponsors: any[] = [];
  selectedPlayer: Player | null = null;
  medicalData: any = {};
  qrInstance: any = null;
  
  qrLibraryLoaded: boolean = false;
  
  constructor(private http: HttpClient) {}
  
  ngOnInit(): void {
    // Load players data
    this.loadPlayers();
    
    // Load QR code library dynamically
    this.loadQRCodeLibrary();
  }

  ngAfterViewInit(): void {
    // Initialize DOM-related functionality
    this.initMobileNav();
    
    // Add this console log to verify it's running
    console.log('Mobile navigation initialized');
    
    // Add random player button functionality for development testing
    const randomBtn = document.getElementById('random-player-btn');
    if (randomBtn) {
      randomBtn.addEventListener('click', () => {
        this.selectRandomPlayer();
      });
      
      // Show random button in development
      if (window.location.hostname === 'localhost') {
        randomBtn.classList.remove('d-none');
      }
    }
  }

  // Listen for window scroll events to change header style
  @HostListener('window:scroll', [])
  onWindowScroll(): void {
    const header = document.querySelector('.header') as HTMLElement;
    if (header) {
      if (window.pageYOffset > 50) {
        header.classList.add('scrolled');
      } else {
        header.classList.remove('scrolled');
      }
    }
  }

 // Load QR Code library with better error handling
  loadQRCodeLibrary(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      // Check if already loaded
      if ((window as any).QRCode) {
        console.log('QR Code library already loaded');
        this.qrLibraryLoaded = true;
        return resolve(true);
      }

      // Create the script element
      const script = document.createElement('script');
      script.src = 'https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js';
      script.integrity = 'sha512-CNgIRecGo7nphbeZ04Sc13ka07paqdeTu0WR1IM4kNcpmBAUSHSQX0FslNhTDadL4O5SAGapGt4FodqL8My0mA==';
      script.crossOrigin = 'anonymous';
      script.referrerPolicy = 'no-referrer';
      
      // Set load handlers
      script.onload = () => {
        console.log('QR Code library loaded successfully');
        this.qrLibraryLoaded = true;
        resolve(true);
      };
      
      script.onerror = (error) => {
        console.error('Failed to load QR Code library:', error);
        reject(false);
      };
      
      // Append to document
      document.head.appendChild(script);
    });
  }

  loadPlayers(): void {
    // In a real app, you would load data from your service
    // For now, we'll create mock data based on the CSV content
    this.http.get('assets/data/players.json')
      .subscribe(
        (data: any) => {
          this.players = data;
          console.log(`Loaded ${this.players.length} players`);
        },
        (error) => {
          console.error('Error loading players:', error);
          // Fallback: Create mock players if data loading fails
          this.createMockPlayers();
        }
      );
  }
  
  createMockPlayers(): void {
    // Based on the content from Players_Dataset.csv
    this.players = [
      {
        player_id: 5, 
        full_name: "Ali Youssef",
        position: "CM", 
        age: 18,
        nationality: "Morocco", 
        club: "Mohammed VI Football Academy",
        medical_history: "No recent injuries",
        fitness_level: 95.13
      },
      {
        player_id: 11, 
        full_name: "Lameck Siame",
        position: "GK", 
        age: 20,
        nationality: "Zambia", 
        club: "ZESCO United",
        medical_history: "Minor injury: Expected recovery in 1-2 weeks",
        fitness_level: 82.35
      },
      {
        player_id: 24, 
        full_name: "Vladyslav Vanat",
        position: "FW", 
        age: 22,
        nationality: "Ukraine", 
        club: "Dynamo Kyiv",
        medical_history: "No recent injuries",
        fitness_level: 96.77
      },
      {
        player_id: 63, 
        full_name: "Victor Osimhen",
        position: "FW", 
        age: 18,
        nationality: "Nigeria", 
        club: "Mavlon FC",
        medical_history: "No recent injuries",
        fitness_level: 75.9
      }
    ];
  }

  async showPlayerQRModal(event: Event): Promise<void> {
  event.preventDefault();
  
  // Select a random player if none is selected
  if (!this.selectedPlayer) {
    this.selectRandomPlayer();
  }
  
  // Generate medical data for this player
  this.generatePlayerMedicalData();
  
  // Show modal first so user sees something happening
  const modalEl = document.getElementById('playerQRModal');
  if (modalEl) {
    const modal = new bootstrap.Modal(modalEl);
    modal.show();
    
    // Set a loading indicator in the QR container
    const qrContainer = document.getElementById('qr-code');
    if (qrContainer) {
      qrContainer.innerHTML = '<div class="d-flex justify-content-center"><div class="spinner-border text-primary" role="status"><span class="visually-hidden">Loading...</span></div></div>';
    }
    
    // Wait a moment to ensure modal is shown before generating QR
    setTimeout(() => {
      this.generateQRCode();
    }, 500);
  }
}
  selectRandomPlayer(): void {
    // Make sure we have players loaded
    if (this.players.length === 0) {
      this.createMockPlayers();
    }
    
    // Select a random player
    const randomIndex = Math.floor(Math.random() * this.players.length);
    this.selectedPlayer = this.players[randomIndex];
  }

  generatePlayerMedicalData(): void {
    if (!this.selectedPlayer) return;
    
    // Use player data to influence medical data generation
    const fitnessLevel = this.selectedPlayer.fitness_level;
    const hasInjury = this.selectedPlayer.medical_history.includes("injury");
    
    // Create medical record
    const medicalRecord = this.createMedicalRecord(fitnessLevel, hasInjury);
    
    // Create recovery plan
    const recoveryPlan = this.createRecoveryPlan(fitnessLevel, hasInjury);
    
    // Create injury prediction
    const injuryPrediction = this.createInjuryPrediction(fitnessLevel, hasInjury);
    
    // Combine all data
    this.medicalData = {
      player: {
        id: this.selectedPlayer.player_id,
        name: this.selectedPlayer.full_name,
        position: this.selectedPlayer.position,
        age: this.selectedPlayer.age,
        nationality: this.selectedPlayer.nationality,
        club: this.selectedPlayer.club
      },
      medicalRecord,
      recoveryPlan,
      injuryPrediction,
      timestamp: new Date().toISOString()
    };
  }

 createMedicalRecord(fitnessLevel: number, hasInjury: boolean): MedicalRecord {
  // Generate a date in the past 30 days
  const examDate = new Date();
  examDate.setDate(examDate.getDate() - Math.floor(Math.random() * 30));
  
  const doctors = [
    "Dr. Ahmed Khalil", "Dr. Sarah Johnson", "Dr. Carlos Oliveira", 
    "Dr. Maria Rodriguez", "Dr. James Thompson"
  ];
  
  // Generate normal or slightly abnormal vital signs based on fitness and injury
  const systolic = Math.floor(110 + (Math.random() * 20));
  const diastolic = Math.floor(70 + (Math.random() * 15));
  const heartRate = Math.floor(55 + (Math.random() * 25));
  const oxygenLevel = fitnessLevel > 85 ? 98 : 95 + Math.floor(Math.random() * 3);
  
  // Replace notes with empty string or minimal content
  const notes = "";  // Empty string instead of conditional text
  
  // Create imaging results based on injury status
  const imagingResults = [];
  if (hasInjury) {
    imagingResults.push("MRI: Minor inflammation in affected area, healing normally");
    if (Math.random() > 0.5) {
      imagingResults.push("X-Ray: No structural damage detected");
    }
  } else {
    imagingResults.push("No imaging studies required at this time");
  }
  
  return {
    lastExamDate: examDate.toISOString().split('T')[0],
    doctorName: doctors[Math.floor(Math.random() * doctors.length)],
    notes: notes,
    vitalSigns: {
      bloodPressure: `${systolic}/${diastolic}`,
      heartRate: heartRate,
      oxygenLevel: oxygenLevel
    },
    medicalTests: {
      ecg: fitnessLevel > 80 ? "Normal" : "Minor irregularities - monitoring recommended",
      bloodwork: "Within normal parameters",
      imaging: imagingResults
    }
  };
}

  createRecoveryPlan(fitnessLevel: number, hasInjury: boolean): RecoveryPlan {
    // Determine phase based on injury and fitness level
    let phase;
    if (hasInjury) {
      phase = fitnessLevel > 85 ? "Late Rehabilitation" : "Mid Rehabilitation";
    } else {
      phase = "Maintenance / Performance Enhancement";
    }
    
    // Generate dates
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - Math.floor(Math.random() * 14));
    
    const endDate = new Date(startDate);
    endDate.setDate(endDate.getDate() + Math.floor(Math.random() * 21) + 7);
    
    // Create exercises based on phase
    const exercises = [];
    
    if (phase.includes("Rehabilitation")) {
      exercises.push({
        name: "Controlled Resistance Training",
        frequency: "3x per week",
        intensity: "Moderate",
        progress: Math.floor(Math.random() * 40) + 40
      });
      
      exercises.push({
        name: "Balance & Proprioception",
        frequency: "Daily",
        intensity: "Low to Moderate",
        progress: Math.floor(Math.random() * 30) + 60
      });
      
      exercises.push({
        name: "Mobility Exercises",
        frequency: "Daily",
        intensity: "Low",
        progress: Math.floor(Math.random() * 20) + 70
      });
    } else {
      exercises.push({
        name: "High Intensity Interval Training",
        frequency: "2x per week",
        intensity: "High",
        progress: Math.floor(Math.random() * 20) + 75
      });
      
      exercises.push({
        name: "Strength & Power Development",
        frequency: "3x per week",
        intensity: "Moderate to High",
        progress: Math.floor(Math.random() * 15) + 80
      });
    }
    
  
    const dietRecommendations = [
  "Increased protein intake: 2g/kg body weight",
  "Focus on anti-inflammatory foods",
  "Adequate hydration: minimum 3L water daily",
  "Pre-training carbohydrate timing: 1-2 hours before"
];
    // Physio focus areas
    const focusAreas = hasInjury ? 
      ["Affected area rehabilitation", "Joint mobility", "Muscle strengthening"] :
      ["Injury prevention", "Recovery optimization", "Performance enhancement"];
    
    return {
      phase: phase,
      startDate: startDate.toISOString().split('T')[0],
      estimatedEndDate: endDate.toISOString().split('T')[0],
      exercises: exercises,
      physioSessions: {
        frequency: hasInjury ? "5x per week" : "2-3x per week",
        focusAreas: focusAreas
      },
  dietRecommendations: dietRecommendations.slice(0, Math.floor(Math.random() * 2) + 2)
    };
  }

  createInjuryPrediction(fitnessLevel: number, hasInjury: boolean): InjuryPrediction {
    // Calculate risk based on fitness level and injury history
    let riskScore = 100 - fitnessLevel; // Base risk on inverse of fitness
    
    // Increase risk if already injured
    if (hasInjury) {
      riskScore += 15;
    }
    
    // Cap at 100
    riskScore = Math.min(riskScore, 100);
    
    // Determine risk level
    let riskLevel;
    if (riskScore < 20) {
      riskLevel = "Low";
    } else if (riskScore < 50) {
      riskLevel = "Moderate";
    } else if (riskScore < 75) {
      riskLevel = "High";
    } else {
      riskLevel = "Very High";
    }
    
    // Determine vulnerable areas based on position
    let vulnerableAreas = [];
    
    if (this.selectedPlayer?.position === "GK") {
      vulnerableAreas = ["Shoulders", "Wrists", "Lower Back"];
      if (Math.random() > 0.6) vulnerableAreas.push("Knee");
    } else if (this.selectedPlayer?.position === "FW") {
      vulnerableAreas = ["Hamstrings", "Ankles", "Calves"];
      if (Math.random() > 0.6) vulnerableAreas.push("Groin");
    } else { // Midfielder or Defender
      vulnerableAreas = ["Knees", "Ankles", "Thigh"];
      if (Math.random() > 0.6) vulnerableAreas.push("Achilles");
    }
    
    // Randomize a bit by removing one area sometimes
    if (Math.random() > 0.7 && vulnerableAreas.length > 2) {
      vulnerableAreas.pop();
    }
    
    // Create preventative measures
    const preventativeMeasures = [
      "Customized warm-up protocol",
      "Regular proprioception training",
      "Core strengthening program",
      "Flexibility focus during recovery sessions",
      "Regular biomechanical assessment",
      "Managed training load",
      "Regular cryotherapy sessions"
    ];
    
    // Choose 3-4 random measures
    const selectedMeasures = [];
    const measureCount = Math.floor(Math.random() * 2) + 3; // 3-4 measures
    
    while (selectedMeasures.length < measureCount) {
      const randomMeasure = preventativeMeasures[Math.floor(Math.random() * preventativeMeasures.length)];
      if (!selectedMeasures.includes(randomMeasure)) {
        selectedMeasures.push(randomMeasure);
      }
    }
    
    // Determine recommended workload
    let recommendedWorkload;
    if (riskScore < 30) {
      recommendedWorkload = "Full training and match participation";
    } else if (riskScore < 60) {
      recommendedWorkload = "Full training with monitored intensity";
    } else if (riskScore < 80) {
      recommendedWorkload = "Modified training (70-80% intensity)";
    } else {
      recommendedWorkload = "Reduced load (50-60% intensity), limited match minutes";
    }
    
    return {
      riskLevel: riskLevel,
      riskScore: Math.round(riskScore),
      vulnerableAreas: vulnerableAreas,
      preventativeMeasures: selectedMeasures,
      recommendedWorkload: recommendedWorkload
    };
  }

generateQRCode(): void {
  const qrContainer = document.getElementById('qr-code');
  if (!qrContainer) return;
  
  // Clear any existing QR code
  qrContainer.innerHTML = '';
  
  try {
    // Instead of embedding the whole PDF, create a simplified data object
    // with just the essential medical information
    const simplifiedData = {
      player: {
        id: this.selectedPlayer?.player_id,
        name: this.selectedPlayer?.full_name,
        position: this.selectedPlayer?.position
      },
      medical: {
        examDate: this.medicalData?.medicalRecord?.lastExamDate,
        doctor: this.medicalData?.medicalRecord?.doctorName,
        vitalSigns: {
          bp: this.medicalData?.medicalRecord?.vitalSigns?.bloodPressure,
          hr: this.medicalData?.medicalRecord?.vitalSigns?.heartRate
        }
      },
      recovery: {
        phase: this.medicalData?.recoveryPlan?.phase,
        completion: this.medicalData?.recoveryPlan?.estimatedEndDate
      },
      risk: {
        level: this.medicalData?.injuryPrediction?.riskLevel,
        score: this.medicalData?.injuryPrediction?.riskScore,
        areas: this.medicalData?.injuryPrediction?.vulnerableAreas?.slice(0, 2)
      },
      timestamp: new Date().toISOString().split('T')[0]
    };
    
    // Convert to string - this will be much smaller than the PDF
    const qrData = JSON.stringify(simplifiedData);
    console.log('QR data size:', qrData.length, 'bytes');
    
    // Create QR code if library is loaded
    const windowObj: any = window;
    if (typeof windowObj.QRCode !== 'undefined') {
      this.qrInstance = new windowObj.QRCode(qrContainer, {
        text: qrData,
        width: 220,
        height: 220,
        colorDark: "#000000",
        colorLight: "#ffffff",
        correctLevel: windowObj.QRCode.CorrectLevel.L  // Use low correction for more capacity
      });
      console.log('QR code generated successfully');
    } else {
      qrContainer.innerHTML = '<div class="alert alert-warning mt-3">QR Code library not loaded. Please try again.</div>';
    }
  } catch (error) {
    console.error('Error generating QR code:', error);
    qrContainer.innerHTML = `<div class="alert alert-danger mt-3">
      <p>Error generating QR code.</p>
      <button class="btn btn-sm btn-outline-primary" onclick="window.location.reload()">Reload Page</button>
    </div>`;
  }
}

generateMedicalPDF(): string {
  // Make sure jsPDF is available
  const windowObj: any = window;
  if (!windowObj.jspdf || !windowObj.jspdf.jsPDF) {
    // Add jsPDF dynamically if not already available
    const script = document.createElement('script');
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
    document.head.appendChild(script);
    
    const autoTableScript = document.createElement('script');
    autoTableScript.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.5.28/jspdf.plugin.autotable.min.js';
    document.head.appendChild(autoTableScript);
    
    console.error('jsPDF library not loaded');
    return '';
  }
  
  try {
    // Create new jsPDF instance
    const { jsPDF } = windowObj.jspdf;
    const doc = new jsPDF();
    
    // Define brand colors - using the requested #ee1e46 as primary color
    const brandRed = [238, 30, 70]; // #ee1e46
    const darkBlue = [41, 50, 65];
    const lightGray = [240, 240, 240];
    const darkGray = [100, 100, 100];

    // Page border with brand color
    doc.setDrawColor(...brandRed);
    doc.setLineWidth(3);
    doc.rect(5, 5, 200, 287);
    
    // Create header with logo-style text
    doc.setFillColor(...brandRed);
    doc.rect(0, 0, 210, 40, 'F');
    
    // Draw a medical cross icon
    doc.setDrawColor(255, 255, 255);
    doc.setLineWidth(3);
    doc.line(20, 20, 30, 20); // Horizontal line
    doc.line(25, 15, 25, 25); // Vertical line

    // Title area
    doc.setTextColor(255, 255, 255);
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(26);
    doc.text('TacticSense', 105, 22, { align: 'center' });
    
    doc.setFontSize(14);
    doc.text('MEDICAL RECORD', 105, 34, { align: 'center' });
    
    // Player INFO CARD
    doc.setDrawColor(200, 200, 200);
    doc.setLineWidth(1);
    doc.setFillColor(250, 250, 250);
    doc.roundedRect(15, 50, 180, 45, 3, 3, 'FD');
    
    // Colored bar for player name
    doc.setFillColor(...brandRed);
    doc.rect(15, 50, 180, 10, 'F');
    
    // Player name on colored bar
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(12);
    doc.setFont('helvetica', 'bold');
    doc.text(`PLAYER: ${this.selectedPlayer?.full_name?.toUpperCase()}`, 20, 57);
    
    // Player ID badge
    doc.setFillColor(50, 50, 50);
    doc.roundedRect(155, 51.5, 35, 7, 2, 2, 'F');
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(9);
    doc.text(`ID #${this.selectedPlayer?.player_id}`, 172.5, 57, { align: 'center' });

    // Player details
    doc.setTextColor(80, 80, 80);
    doc.setFontSize(10);
    doc.setFont('helvetica', 'normal');
    
    const detailsX = 20;
    doc.text(`Position: ${this.selectedPlayer?.position}`, detailsX, 70);
    doc.text(`Age: ${this.selectedPlayer?.age} years`, detailsX + 60, 70);
    doc.text(`Nationality: ${this.selectedPlayer?.nationality}`, detailsX, 80);
    doc.text(`Club: ${this.selectedPlayer?.club}`, detailsX + 60, 80);
    
   // ===== MEDICAL RECORD SECTION =====
const medicalY = 110;

// Section title
doc.setDrawColor(...brandRed);
doc.setLineWidth(0.5);
doc.line(15, medicalY - 10, 195, medicalY - 10);

doc.setFillColor(...brandRed);
doc.rect(15, medicalY - 10, 50, 7, 'F');

doc.setFont('helvetica', 'bold');
doc.setTextColor(255, 255, 255);
doc.setFontSize(11);
doc.text('MEDICAL RECORD', 20, medicalY - 5);

// Main content
doc.setFont('helvetica', 'normal');
doc.setTextColor(60, 60, 60);
doc.setFontSize(10);

// Medical stats in a formatted table
// Medical stats in a formatted table
doc.autoTable({
  startY: medicalY,
  head: [['Last Examination', 'Physician', 'Blood Pressure', 'Heart Rate', 'Oxygen']],
  body: [[
    this.medicalData?.medicalRecord?.lastExamDate || 'N/A',
    this.medicalData?.medicalRecord?.doctorName || 'N/A',
    this.medicalData?.medicalRecord?.vitalSigns?.bloodPressure || 'N/A',
    `${this.medicalData?.medicalRecord?.vitalSigns?.heartRate || 'N/A'} bpm`,
    `${this.medicalData?.medicalRecord?.vitalSigns?.oxygenLevel || 'N/A'}%`
  ]],
  headStyles: {
    fillColor: [240, 240, 240],
    textColor: [80, 80, 80],
    fontSize: 8,
    fontStyle: 'bold'
  },
  styles: { 
    fontSize: 9,
    cellPadding: 4
  },
  theme: 'grid',
  margin: { left: 15, right: 15 }
});

    
// ===== RECOVERY PLAN SECTION =====
// Position directly after the medical record table with no Notes section
const recoveryY = doc.lastAutoTable.finalY + 20;
    
 // Section title with brand color
doc.setDrawColor(...brandRed);
doc.setLineWidth(0.5);
doc.line(15, recoveryY, 195, recoveryY);
    
    doc.setFillColor(...brandRed);
    doc.rect(15, recoveryY, 50, 7, 'F');
    
    doc.setFont('helvetica', 'bold');
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(11);
    doc.text('RECOVERY PLAN', 20, recoveryY + 5);
    
    // Phase indicator
    doc.setFont('helvetica', 'bold');
    doc.setTextColor(60, 60, 60);
    doc.setFontSize(10);
    doc.text(`Current Phase: ${this.medicalData?.recoveryPlan?.phase}`, 20, recoveryY + 15);
    
    doc.setFont('helvetica', 'normal');
    doc.setTextColor(80, 80, 80);
    doc.text(`Start Date: ${this.medicalData?.recoveryPlan?.startDate}`, 20, recoveryY + 25);
    doc.text(`Target Completion: ${this.medicalData?.recoveryPlan?.estimatedEndDate}`, 120, recoveryY + 25);
    
    // Exercise table with progress indicators
    const exercises = this.medicalData?.recoveryPlan?.exercises || [];
    if (exercises.length > 0) {
      // Convert exercises to table format with colored progress cells
      const exerciseTableBody = exercises.map(ex => [
        ex.name,
        ex.frequency,
        ex.intensity,
        { 
          content: `${ex.progress}%`,
          styles: {
            fillColor: this.getProgressColor(ex.progress),
            textColor: [255, 255, 255],
            fontStyle: 'bold'
          }
        }
      ]);
      
      doc.autoTable({
        startY: recoveryY + 30,
        head: [['Exercise', 'Frequency', 'Intensity', 'Progress']],
        body: exerciseTableBody,
        headStyles: {
          fillColor: [245, 245, 245],
          textColor: [80, 80, 80],
          fontStyle: 'bold'
        },
        columnStyles: {
          0: { fontStyle: 'bold' },
          3: { halign: 'center' }
        },
        styles: { fontSize: 9 },
        theme: 'grid',
        margin: { left: 15, right: 15 }
      });
    }
    
    // ===== INJURY RISK SECTION =====
    // Calculate injuryY directly from the exercise table position
    const injuryY = doc.lastAutoTable.finalY + 15; // Reduced padding between sections
    
    // Section title with brand color
    doc.setDrawColor(...brandRed);
    doc.setLineWidth(0.5);
    doc.line(15, injuryY, 195, injuryY);
    
    doc.setFillColor(...brandRed);
    doc.rect(15, injuryY, 50, 7, 'F');
    
    doc.setFont('helvetica', 'bold');
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(11);
    doc.text('INJURY RISK', 20, injuryY + 5);
    
    // Risk score visualization
    const riskScore = this.medicalData?.injuryPrediction?.riskScore || 0;
    const riskLevel = this.medicalData?.injuryPrediction?.riskLevel || 'N/A';
    
    // Risk meter background - make slightly smaller to save space
    doc.setFillColor(240, 240, 240);
    doc.roundedRect(20, injuryY + 15, 170, 10, 3, 3, 'F');
    
    // Risk meter foreground (colored based on risk)
    const width = Math.min(170, (riskScore / 100) * 170);
    doc.setFillColor(...this.getRiskColor(riskScore));
    doc.roundedRect(20, injuryY + 15, width, 10, 3, 3, 'F');
    
    // Risk score text
    doc.setTextColor(255, 255, 255);
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(10);
    
    if (width > 30) { // Only show text if bar is wide enough
      doc.text(`${riskScore}%`, 25, injuryY + 22);
    } else {
      doc.setTextColor(80, 80, 80);
      doc.text(`${riskScore}%`, 25, injuryY + 32);
    }
    
    // Risk level and areas on same line to save space
    doc.setTextColor(80, 80, 80);
    doc.setFont('helvetica', 'bold');
    doc.text(`Risk Level: ${riskLevel}`, 120, injuryY + 32);
    

// Vulnerable areas on same line
const areas = this.medicalData?.injuryPrediction?.vulnerableAreas || [];
doc.setFont('helvetica', 'bold');
doc.setFontSize(9);
doc.text('Vulnerable Areas:', 20, injuryY + 42);
doc.setFont('helvetica', 'normal');
doc.text(areas.join(', '), 85, injuryY + 42);

// Calculate available space and ensure recommendation fits
const availableHeight = 255 - (injuryY + 50); // Calculate space before footer
const recommendation = this.medicalData?.injuryPrediction?.recommendedWorkload || 'N/A';
const wrappedRecommendation = doc.splitTextToSize(recommendation, 170);

// Check if recommendation will fit in available space
if (wrappedRecommendation.length * 5 > availableHeight) {
  // Option 1: We could add a new page, but for simplicity:
  // Option 2: Truncate text to fit if too long
  const maxLines = Math.floor(availableHeight / 5) - 1;
  wrappedRecommendation.splice(maxLines);
  wrappedRecommendation[maxLines - 1] += '...';
}

// Recommendation section
doc.setFont('helvetica', 'bold');
doc.text('Recommendation:', 20, injuryY + 52);
doc.setFont('helvetica', 'normal');
doc.text(wrappedRecommendation, 20, injuryY + 60);

// Move the footer down to allow more space for content
doc.setDrawColor(...brandRed);
doc.setLineWidth(1);
doc.line(15, 275, 195, 275);

doc.setTextColor(...darkGray);
doc.setFontSize(8);
doc.text(`Generated: ${new Date().toLocaleString()}`, 20, 285);

doc.setFont('helvetica', 'bold');
doc.setTextColor(...brandRed);
doc.text('CONFIDENTIAL - FOR MEDICAL STAFF ONLY', 195, 285, { align: 'right' });
    // Return the PDF as a data URL
    return doc.output('dataurlstring');
  } catch (error) {
    console.error('Error generating PDF:', error);
    return '';
  }
}

// Helper function to get color for progress bar based on completion percentage
private getProgressColor(progress: number): number[] {
  if (progress < 25) return [231, 76, 60]; // Red - #e74c3c
  if (progress < 50) return [230, 126, 34]; // Orange - #e67e22
  if (progress < 75) return [241, 196, 15]; // Yellow - #f1c40f
  return [46, 204, 113]; // Green - #2ecc71
}

// Helper function to get color for risk indicator
private getRiskColor(risk: number): number[] {
  if (risk < 25) return [46, 204, 113]; // Green - #2ecc71
  if (risk < 50) return [241, 196, 15]; // Yellow - #f1c40f
  if (risk < 75) return [230, 126, 34]; // Orange - #e67e22
  return [231, 76, 60]; // Red - #e74c3c - close to your brand color #ee1e46
}
// Open the generated PDF in a new tab
openMedicalPDF(): void {
  const pdfDataUrl = this.generateMedicalPDF();
  if (pdfDataUrl) {
    window.open(pdfDataUrl, '_blank');
  }
}
  downloadQR(): void {
  const canvas = document.querySelector('#qr-code canvas');
  if (canvas) {
    const link = document.createElement('a');
    const playerName = this.selectedPlayer?.full_name.replace(/\s+/g, '_') || 'player';
    link.download = `medical_qr_${playerName}_pdf.png`;
    link.href = (canvas as HTMLCanvasElement).toDataURL('image/png');
    link.click();
  } else {
    console.error('QR canvas element not found');
  }
}

  // Initialize mobile navigation functionality
  private initMobileNav(): void {
    // For mobile navigation toggle
    const mobileNavToggle = document.querySelector('.mobile-nav-toggle');
    const navmenu = document.getElementById('navmenu');
    
    console.log('Toggle element:', mobileNavToggle);
    console.log('Nav menu element:', navmenu);
    
    if (mobileNavToggle && navmenu) {
      // Remove any existing listeners (cleanup)
      const newToggle = mobileNavToggle.cloneNode(true) as HTMLElement; 
      mobileNavToggle.parentNode?.replaceChild(newToggle, mobileNavToggle);
      
      // Add the event listener to the new element
      newToggle.addEventListener('click', (e) => {
        e.preventDefault();
        console.log('Toggle clicked');
        navmenu.classList.toggle('active');
        newToggle.classList.toggle('bi-list');
        newToggle.classList.toggle('bi-x');
      });
    }

    // Handle dropdown toggles on mobile
    const dropdownLinks = document.querySelectorAll('.navmenu .dropdown > a');
    dropdownLinks.forEach(link => {
      link.addEventListener('click', function(e) {
        if (window.innerWidth < 1200) {
          e.preventDefault();
          (this as any).nextElementSibling.classList.toggle('dropdown-active');
          (this as any).parentElement.classList.toggle('active');
        }
      });
    });
  }
}