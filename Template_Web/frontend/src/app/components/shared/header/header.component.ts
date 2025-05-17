import { Component, OnInit, HostListener, AfterViewInit } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';

// Declare libraries needed for the header
declare var $: any;
declare var bootstrap: any;

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit {
    players = [];
  clubs = [];
  agents = [];
  sponsors = [];
  
  
  constructor(private sanitizer: DomSanitizer) {}

  ngOnInit(): void {
    // Load QR code library
    this.loadQRCodeLibrary();
    // Load the sample data
    this.loadSampleData();
  }
    loadQRCodeLibrary() {
    if (typeof window['QRCode'] === 'undefined') {
      const script = document.createElement('script');
      script.src = 'https://cdn.jsdelivr.net/npm/qrcode.js/qrcode.min.js';
      document.head.appendChild(script);
    }
  }
   loadSampleData() {
    // Load sample player data
    this.players = [
      { id: 1, name: "Mamadou Diop", position: "GK", nationality: "Senegal", club: "Linguère" },
      { id: 24, name: "Vladyslav Vanat", position: "FW", nationality: "Ukraine", club: "Dynamo Kyiv" },
      { id: 5, name: "Ali Youssef", position: "CM", nationality: "Morocco", club: "Mohammed VI Academy" },
      { id: 11, name: "Lameck Siame", position: "GK", nationality: "Zambia", club: "ZESCO United" },
      { id: 63, name: "Victor Osimhen", position: "FW", nationality: "Nigeria", club: "Mavlon FC" }
    ];
    
    // Load sample club data
    this.clubs = [
      { id: 265, name: "1o de Agosto", country: "Angola", league: "Girabola", stadium: "Estadio 11 de Novembro" },
      { id: 61, name: "Al Ahly", country: "Egypt", league: "Egyptian Premier League", stadium: "Cairo International Stadium" },
      { id: 272, name: "Al Hilal Omdurman", country: "Sudan", league: "Sudan Premier League", stadium: "Al Merreikh Stadium" }
    ];
    
    // Load sample agent data from the recruiting agents dataset
    this.agents = [
      { id: 7, name: "Jorge Mendes", region: "Portugal", experience: "25 years" },
      { id: 12, name: "Giovanni Branchini", region: "Italy", experience: "30 years" },
      { id: 20, name: "Paul Stretford", region: "United Kingdom", experience: "25 years" }
    ];
    
    // Load sample sponsor data
    this.sponsors = [
      { id: 1, name: "TotalEnergies", industry: "Energy", deals: "Title sponsor of CAF competitions" },
      { id: 10, name: "Orange", industry: "Telecommunications", deals: "Official sponsor of CAF competitions" },
      { id: 34, name: "MultiChoice (DStv)", industry: "Media & Entertainment", deals: "Broadcast partner for CAF" }
    ];
  }
 showNetworkQRModal(event: Event) {
    event.preventDefault();
    
    // Choose a random entity type
    const entityTypes = ['player', 'club', 'agent', 'sponsor'];
    const randomType = entityTypes[Math.floor(Math.random() * entityTypes.length)];
    
    let entity;
    let subtitle = '';
    
    // Select random entity based on type
    switch(randomType) {
      case 'player':
        entity = this.players[Math.floor(Math.random() * this.players.length)];
        subtitle = entity.position + (entity.position.length === 2 ? "1" : "");
        break;
      case 'club':
        entity = this.clubs[Math.floor(Math.random() * this.clubs.length)];
        subtitle = entity.league;
        break;
      case 'agent':
        entity = this.agents[Math.floor(Math.random() * this.agents.length)];
        subtitle = entity.experience;
        break;
      case 'sponsor':
        entity = this.sponsors[Math.floor(Math.random() * this.sponsors.length)];
        subtitle = entity.industry;
        break;
    }
    
    // Update modal with entity information
    document.getElementById('qr-entity-number').textContent = '#' + entity.id;
    document.getElementById('qr-entity-name').textContent = entity.name.toUpperCase();
    document.getElementById('qr-entity-position').textContent = subtitle;
    
    // Generate QR code
    const qrContainer = document.getElementById('qr-code');
    qrContainer.innerHTML = '';
    
    // Define the QR content - stringify the entity with relevant info
    const qrData = JSON.stringify({
      id: entity.id,
      type: randomType,
      name: entity.name,
      info: subtitle,
      timestamp: Date.now()
    });
    
    // Create QR code (using the library)
    setTimeout(() => {
      if (typeof window['QRCode'] !== 'undefined') {
        new window['QRCode'](qrContainer, {
          text: qrData,
          width: 200,
          height: 200,
          colorDark: "#000000",
          colorLight: "#ffffff",
          correctLevel: window['QRCode'].CorrectLevel.H
        });
      } else {
        qrContainer.innerHTML = '<div class="text-center my-5">QR Code Loading...</div>';
      }
    }, 500);
    
    // Set up download button
    document.getElementById('download-qr-btn').onclick = () => {
      const canvas = qrContainer.querySelector('canvas');
      if (canvas) {
        const link = document.createElement('a');
        link.download = `${randomType}-${entity.id}-qrcode.png`;
        link.href = canvas.toDataURL('image/png');
        link.click();
      }
    };
    
    // Show the modal using Bootstrap
    const modal = new bootstrap.Modal(document.getElementById('networkQRModal'));
    modal.show();
  }

 ngAfterViewInit() {
    // This is the right place to initialize DOM-related functionality
    this.initMobileNav();
    
    // Add this console log to verify it's running
    console.log('Mobile navigation initialized');
  }

  // Listen for window scroll events to change header style
  @HostListener('window:scroll', [])
  onWindowScroll() {
    const header = document.querySelector('.header') as HTMLElement;
    if (header) {
      if (window.pageYOffset > 50) {
        header.classList.add('scrolled');
      } else {
        header.classList.remove('scrolled');
      }
    }
  }

  // Initialize mobile navigation functionality
 // Initialize mobile navigation functionality
private initMobileNav(): void {
  // For mobile navigation toggle
  const mobileNavToggle = document.querySelector('.mobile-nav-toggle');
  const navmenu = document.getElementById('navmenu');
  
  console.log('Toggle element:', mobileNavToggle);
  console.log('Nav menu element:', navmenu);
  
  if (mobileNavToggle && navmenu) {
    // Remove any existing listeners (cleanup)
    const newToggle = mobileNavToggle.cloneNode(true) as HTMLElement; // Add type assertion here
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