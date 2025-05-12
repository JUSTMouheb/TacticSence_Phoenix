import { Component, OnInit, AfterViewInit } from '@angular/core';
import { LoginService } from 'src/app/services/login.service';
import { AuthService } from 'src/app/services/auth.service';
declare var bootstrap: any;

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit, AfterViewInit {
  showPassword = false;
  loginModal: any;
  isSignUpMode = false;
  
constructor(
  private loginService: LoginService,
  public authService: AuthService  // Change from private to public
) {}
  
  ngOnInit(): void {
    // Handle initialization tasks
  }
  
  ngAfterViewInit(): void {
    // Initialize the modal after view is ready
    const modalElement = document.getElementById('loginModal');
    if (modalElement) {
      this.loginModal = new bootstrap.Modal(modalElement);
      
      // Subscribe to the login service to open modal when requested
      this.loginService.showLoginModal$.subscribe(() => {
        this.isSignUpMode = false; // Reset to login form when opening
        this.loginModal.show();
      });
    }
  }
  
signInWithFacebook(): void {
  this.authService.signInWithFacebook();
  this.loginModal.hide();
}
  
  signInWithGoogle(): void {
    this.authService.signInWithGoogle();
    this.loginModal.hide();
  }
  
  signInWithLinkedIn(): void {
    this.authService.signInWithLinkedIn();
    this.loginModal.hide();
  }
  
  // Toggle between sign-in and sign-up modes
  toggleMode(): void {
    this.isSignUpMode = !this.isSignUpMode;
  }
  
  togglePasswordVisibility(): void {
    this.showPassword = !this.showPassword;
  }
  // Add this method to your LoginComponent class:

logout(): void {
  this.authService.logout().subscribe({
    next: () => {
      // Additional logic after logout if needed
      console.log('Logged out successfully');
    },
    error: (err) => {
      console.error('Logout error:', err);
    }
  });
}
}