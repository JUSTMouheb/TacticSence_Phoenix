import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-footer',
  templateUrl: './footer.component.html',
  styleUrls: ['./footer.component.css']  // or .scss if you're using SCSS
})
export class FooterComponent implements OnInit {
  currentYear: number = new Date().getFullYear();
  
  constructor() { }

  ngOnInit(): void {
    // Initialize component
    this.initScrollTop();
  }

  private initScrollTop(): void {
    const scrollTop = document.querySelector('#scroll-top') as HTMLElement;
    if (!scrollTop) return;

    // Toggle visibility based on scroll position
    const toggleScrollTop = () => {
      if (window.scrollY > 100) {
        scrollTop.classList.add('active');
      } else {
        scrollTop.classList.remove('active');
      }
    };

    // Add event listener for scrolling
    window.addEventListener('scroll', toggleScrollTop);

    // Initial check
    toggleScrollTop();
    
    // Handle click on scroll top button
    scrollTop.addEventListener('click', (e) => {
      e.preventDefault();
      window.scrollTo({
        top: 0,
        behavior: 'smooth'
      });
    });
  }
}