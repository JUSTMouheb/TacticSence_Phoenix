import { Component, OnInit } from '@angular/core';
// Fix the import path - remove .ts and fix the missing quote
import { LoginService } from '../../../services/login.service';
import { FraudDetectionService } from '../../../services/fraud_detection.service';
declare var AOS: any;
declare var GLightbox: any;

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {
  constructor(private loginService: LoginService) {}

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
  
  openLogin(event: Event) {
    event.preventDefault();
    this.loginService.openLoginModal();
  }
}