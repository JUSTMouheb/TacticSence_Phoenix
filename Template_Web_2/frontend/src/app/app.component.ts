import { Component, OnInit, AfterViewInit } from '@angular/core';

// Declare any external libraries that your component will use
declare var AOS: any;
declare var GLightbox: any;
declare var Swiper: any;

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit, AfterViewInit {
  title = 'TacticSense';

  constructor() { }

  ngOnInit(): void {
    // Initialize functionality after the component is loaded
    this.handlePreloader();
    this.initAOS();
    this.initScrollTop();
    this.initOtherComponents();
  }
  
  ngAfterViewInit(): void {
    // Initialize any post-render functionality if needed
  }

  // Initialize all UI components and libraries
  private initializeComponents(): void {
    // Initialize AOS (Animate On Scroll) if available
    this.initAOS();

    // Handle preloader
    this.handlePreloader();

    // Initialize scroll top button
    this.initScrollTop();

    // Initialize other UI components
    this.initOtherComponents();
  }

  // Initialize AOS library
  private initAOS(): void {
    // Check if AOS is available
    if (typeof AOS !== 'undefined') {
      AOS.init({
        duration: 1000,
        easing: 'ease-in-out',
        once: true,
        mirror: false
      });
    }
  }

  // Handle the preloader element
  private handlePreloader(): void {
    const preloader = document.getElementById('preloader');
    if (!preloader) return;

    // Hide preloader after page loads
    window.addEventListener('load', () => {
      setTimeout(() => {
        preloader.classList.add('loaded');
      }, 1000);
      setTimeout(() => {
        if (preloader.parentNode) {
          preloader.parentNode.removeChild(preloader);
        }
      }, 2000);
    });
  }

  // Initialize scroll top button
  private initScrollTop(): void {
    const scrollTop = document.querySelector('.scroll-top') as HTMLElement;
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

  // Initialize other UI components
  private initOtherComponents(): void {
    // Initialize GLightbox if available
    if (typeof GLightbox !== 'undefined') {
      const lightbox = document.querySelector('.glightbox');
      if (lightbox) {
        GLightbox({
          selector: '.glightbox'
        });
      }
    }

    // Initialize Swiper sliders if available
    if (typeof Swiper !== 'undefined') {
      const sliders = document.querySelectorAll('.swiper');
      if (sliders.length > 0) {
        sliders.forEach((slider: Element) => {
          new Swiper(slider, {
            speed: 600,
            loop: true,
            autoplay: {
              delay: 5000,
              disableOnInteraction: false
            },
            slidesPerView: 'auto',
            pagination: {
              el: '.swiper-pagination',
              type: 'bullets',
              clickable: true
            },
            navigation: {
              nextEl: '.swiper-button-next',
              prevEl: '.swiper-button-prev',
            }
          });
        });
      }
    }
  }
}