import { Component, OnInit, HostListener, AfterViewInit } from '@angular/core';

// Declare libraries needed for the header
declare var $: any;

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit {
  
  constructor() { }

  ngOnInit(): void {

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