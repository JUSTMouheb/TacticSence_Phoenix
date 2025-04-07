import { Component, OnInit } from '@angular/core';

declare var AOS: any;
declare var GLightbox: any;

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css'] // or .scss if you use SCSS
})
export class HomeComponent implements OnInit {

  constructor() { }

  ngOnInit(): void {
    // Initialize AOS
    AOS.init({
      duration: 1000,
      easing: 'ease-in-out',
      once: true,
      mirror: false
    });

    // Initialize GLightbox
    const glightbox = GLightbox({
      selector: '.glightbox'
    });
  }
}