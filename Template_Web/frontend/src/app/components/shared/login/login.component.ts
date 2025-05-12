import { Component, OnInit, AfterViewInit, ViewChild, ElementRef } from '@angular/core';
import { LoginService } from 'src/app/services/login.service';
import { AuthService } from 'src/app/services/auth.service';
import { VerificationService } from 'src/app/services/verification.service';
declare var bootstrap: any;

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit, AfterViewInit {
  // Login properties
  showPassword = false;
  loginModal: any;
  isSignUpMode = false;
  loginEmail: string = '';
  loginPassword: string = '';
  
  // Signup and verification properties
  signupStep = 1;
  signupRole = 'Player';
  availableRoles = ['Player', 'Agent', 'Club Staff', 'Service Provider'];
  
  // Document verification
  idDocument: File | null = null;
  roleDocument: File | null = null;
  idVerified = false;
  roleDocVerified = false;
  idVerificationInProgress = false;
  roleDocVerificationInProgress = false;
  
  // Webcam references
  @ViewChild('webcamVideo') webcamVideo: ElementRef;
  @ViewChild('canvas') canvas: ElementRef;
  
  // Face verification
  isWebcamActive = false;
  selfieBase64: string | null = null;
  verificationInProgress = false;
  verificationAttempted = false;
  faceVerified = false;
  stream: MediaStream | null = null;
  
  constructor(
    private loginService: LoginService,
    public authService: AuthService,
    private verificationService: VerificationService
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
  
 
  
  // REGULAR LOGIN/LOGOUT
  login(): void {
    if (!this.loginEmail || !this.loginPassword) {
      alert('Please enter both email and password');
      return;
    }
    
    this.authService.login(this.loginEmail, this.loginPassword).subscribe({
      next: (response) => {
        console.log('Login successful');
        this.loginModal.hide();
      },
      error: (err) => {
        console.error('Login error:', err);
        alert('Login failed. Please check your credentials and try again.');
      }
    });
  }
  
  logout(): void {
    this.authService.logout().subscribe({
      next: () => {
        console.log('Logged out successfully');
      },
      error: (err) => {
        console.error('Logout error:', err);
      }
    });
  }
  
   forgotPassword(): void {
    // Implement password reset functionality
    const email = prompt('Please enter your email address:');
    if (email) {
      this.authService.resetPassword(email).subscribe({
        next: () => {
          alert('Password reset instructions have been sent to your email');
        },
        error: (err) => {
          console.error('Password reset error:', err);
          alert('Error sending reset instructions. Please try again.');
        }
      });
    }
  }
   // UI TOGGLES
  // Keep only this implementation of toggleMode() and remove the duplicate one
  toggleMode(): void {
    this.isSignUpMode = !this.isSignUpMode;
  }
  togglePasswordVisibility(): void {
    this.showPassword = !this.showPassword;
  }
  
  // SIGNUP FLOW
  nextStep(): void {
    if (this.signupStep === 1) {
      // Validate role selection
      if (!this.signupRole) {
        alert('Please select a role');
        return;
      }
      this.signupStep = 2;
    } else if (this.signupStep === 2) {
      // Validate document uploads
      if (!this.idVerified || !this.roleDocVerified) {
        alert('Please upload and verify both documents');
        return;
      }
      this.signupStep = 3;
    } else if (this.signupStep === 3) {
      // Validate face verification
      if (!this.faceVerified) {
        alert('Please complete face verification');
        return;
      }
      this.signupStep = 4;
    }
  }
  
  prevStep(): void {
    if (this.signupStep > 1) {
      this.signupStep--;
    }
    
    // Stop webcam if moving away from webcam step
    if (this.signupStep !== 3 && this.isWebcamActive) {
      this.stopWebcam();
    }
  }
  
  // DOCUMENT HANDLING
onIdDocumentSelected(event: any): void {
  const files = event.target.files;
  if (files && files.length > 0) {
    // Clear the input value of the other file input to ensure they don't share state
    const roleFileInput = document.getElementById('roleFile') as HTMLInputElement;
    if (roleFileInput && roleFileInput.value && this.roleDocument === null) {
      roleFileInput.value = '';
    }
    
    // Set the ID document and verify it
    this.idDocument = files[0];
    
    // Log to confirm we're in the correct handler
    console.log('ID document selected:', this.idDocument.name);
    console.log('Role document state:', this.roleDocument?.name || 'none');
    
    this.verifyIdDocument();
  }
}
// Update the onRoleDocumentSelected method
onRoleDocumentSelected(event: any): void {
  const files = event.target.files;
  if (files && files.length > 0) {
    // Clear the input value of the other file input to ensure they don't share state
    const idFileInput = document.getElementById('idFile') as HTMLInputElement;
    if (idFileInput && idFileInput.value && this.idDocument === null) {
      idFileInput.value = '';
    }
    
    // Set the role document and verify it
    this.roleDocument = files[0];
    
    // Log to confirm we're in the correct handler
    console.log('Role document selected:', this.roleDocument.name);
    console.log('ID document state:', this.idDocument?.name || 'none');
    
    this.verifyRoleDocument();
  }
}

  verifyIdDocument(): void {
  if (!this.idDocument) return;
  
  this.idVerificationInProgress = true;
  
  console.log('Starting document verification for:', this.idDocument.name);
  
  this.verificationService.verifyIdDocument(this.idDocument).subscribe({
    next: (result) => {
      console.log('ID verification complete result:', result);
      
      // Check if result has expected format
      if (result && typeof result.valid !== 'undefined') {
        this.idVerified = result.valid;
      } else {
        console.error('Unexpected API response format:', result);
        this.idVerified = false;
      }
      
      this.idVerificationInProgress = false;
    },
    error: (err) => {
      console.error('ID verification error:', err);
      this.idVerified = false;
      this.idVerificationInProgress = false;
      
      // Show error message to user
      alert('Error verifying ID: ' + (err.message || 'Unknown error'));
    }
  });
}
  
  verifyRoleDocument(): void {
    if (!this.roleDocument) return;
    
    this.roleDocVerificationInProgress = true;
    
    this.verificationService.verifyRoleDocument(this.roleDocument, this.signupRole).subscribe({
      next: (result) => {
        console.log('Role document verification result:', result);
        this.roleDocVerified = result.verified;
        this.roleDocVerificationInProgress = false;
      },
      error: (err) => {
        console.error('Role document verification error:', err);
        this.roleDocVerified = false;
        this.roleDocVerificationInProgress = false;
      }
    });
  }
  
  // WEBCAM HANDLING
  startWebcam(): void {
    if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
      navigator.mediaDevices.getUserMedia({ video: true })
        .then((stream) => {
          this.stream = stream;
          this.isWebcamActive = true;
          setTimeout(() => {
            if (this.webcamVideo && this.webcamVideo.nativeElement) {
              this.webcamVideo.nativeElement.srcObject = stream;
              this.webcamVideo.nativeElement.play();
            }
          }, 100);
        })
        .catch((err) => {
          console.error('Error accessing webcam:', err);
          alert('Unable to access the webcam. Please ensure you have granted permission.');
        });
    } else {
      alert('Your browser does not support webcam access.');
    }
  }
  
  stopWebcam(): void {
    if (this.stream) {
      this.stream.getTracks().forEach(track => track.stop());
      this.stream = null;
    }
    this.isWebcamActive = false;
    
    if (this.webcamVideo?.nativeElement) {
      this.webcamVideo.nativeElement.srcObject = null;
    }
  }
  
  captureImage(): void {
    if (!this.isWebcamActive || !this.webcamVideo?.nativeElement) return;
    
    const video = this.webcamVideo.nativeElement;
    const canvas = this.canvas.nativeElement;
    const context = canvas.getContext('2d');
    
    // Set canvas dimensions to match video
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    
    // Draw the current video frame onto the canvas
    context.drawImage(video, 0, 0, canvas.width, canvas.height);
    
    // Convert to base64
    this.selfieBase64 = canvas.toDataURL('image/jpeg');
    
    // Stop webcam
    this.stopWebcam();
    
    // Verify face
    this.verifyFaceMatch();
  }
  
  verifyFaceMatch(): void {
    if (!this.selfieBase64 || !this.idDocument) return;
    
    this.verificationInProgress = true;
    this.verificationAttempted = false;
    
    this.verificationService.verifyFaceMatch(this.idDocument, this.selfieBase64).subscribe({
      next: (result) => {
        console.log('Face verification result:', result);
        this.faceVerified = result.match;
        this.verificationInProgress = false;
        this.verificationAttempted = true;
      },
      error: (err) => {
        console.error('Face verification error:', err);
        this.faceVerified = false;
        this.verificationInProgress = false;
        this.verificationAttempted = true;
      }
    });
  }
  
  // COMPLETION AND SUCCESS
  completeVerification(): void {
    if (this.faceVerified) {
      this.signupStep = 4;
    }
  }
  
  loginVerified(): void {
    // Here, handle the successful verification
    console.log('User verified with role:', this.signupRole);
    
    // You could register the user here if needed
    // this.authService.registerVerifiedUser(this.signupRole).subscribe(...)
    
    // Or you could set a flag in localStorage to remember the verification
    localStorage.setItem('userVerified', 'true');
    localStorage.setItem('userRole', this.signupRole);
    
    // Close the modal
    if (this.loginModal) {
      this.loginModal.hide();
    }
    
    // Redirect user to appropriate page based on role
    // Since you don't want to use Router, you can use window.location
    switch(this.signupRole) {
      case 'Player':
        window.location.href = '/player-dashboard';
        break;
      case 'Agent':
        window.location.href = '/agent-dashboard';
        break;
      case 'Club Staff':
        window.location.href = '/club-dashboard';
        break;
      case 'Service Provider':
        window.location.href = '/service-provider-dashboard';
        break;
      default:
        window.location.href = '/home';
    }
  }
  
  resetSignupForm(): void {
    this.signupStep = 1;
    this.signupRole = 'Player';
    this.idDocument = null;
    this.roleDocument = null;
    this.idVerified = false;
    this.roleDocVerified = false;
    this.selfieBase64 = null;
    this.faceVerified = false;
    
    // Make sure webcam is stopped
    if (this.isWebcamActive) {
      this.stopWebcam();
    }
  }
  
  // HELPER FUNCTIONS
  getRoleDocumentDescription(): string {
    switch(this.signupRole) {
      case 'Player':
        return 'Upload your player registration card or official team document';
      case 'Agent':
        return 'Upload your FIFA agent license or accreditation document';
      case 'Club Staff':
        return 'Upload your club staff ID or professional certification';
      case 'Service Provider':
        return 'Upload your business registration or service provider credentials';
      default:
        return 'Please select a role to see document requirements';
    }
  }
  
  getDocumentLabel(): string {
    switch(this.signupRole) {
      case 'Player':
        return 'Player License';
      case 'Agent':
        return 'FIFA Agent License';
      case 'Club Staff':
        return 'Professional Certification';
      case 'Service Provider':
        return 'Business License';
      default:
        return 'Role Document';
    }
  }
}