import { Component, OnInit, HostListener } from '@angular/core';

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
    // Initialize header-specific functionality
    this.initMobileNav();
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
  private initMobileNav(): void {
    // For mobile navigation toggle
    const mobileNavToggle = document.querySelector('.mobile-nav-toggle');
    const navmenu = document.getElementById('navmenu');
    
    if (mobileNavToggle && navmenu) {
      mobileNavToggle.addEventListener('click', () => {
        navmenu.classList.toggle('active');
        mobileNavToggle.classList.toggle('bi-list');
        mobileNavToggle.classList.toggle('bi-x');
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