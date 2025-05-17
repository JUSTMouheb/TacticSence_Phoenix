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
  // Add these properties to your component class
signupName: string = '';
signupPassword: string = '';
confirmPassword: string = '';
showSignupPassword: boolean = false;
// Add at the top of your login.component.ts class
registrationInProgress = false;
registrationError = '';
signupEmail = ''; // If you don't already have this
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
  // Add these missing properties
  userName: string = '';
  userAvatar: string | null = null;
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
    const user = this.authService.getCurrentUserSync();
    if (user) {
      this.userName = user.name;
      this.userAvatar = user.prefs?.avatar || null;
    }
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
    this.authService.logout();
    // Use window.location instead of Router
    window.location.href = '/';
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

  
// Add these methods to your component class
toggleSignupPasswordVisibility(): void {
  this.showSignupPassword = !this.showSignupPassword;
}

passwordsDoNotMatch(): boolean {
  return this.signupPassword && this.confirmPassword && 
         this.signupPassword !== this.confirmPassword;
}
  
// Update completeVerification method
completeVerification() {
  // Check that we have all required fields
  if (!this.signupName || !this.signupEmail || !this.signupPassword || this.passwordsDoNotMatch()) {
    // Show specific error message
    if (!this.signupEmail) {
      this.registrationError = "Email address is required";
    } else if (!this.signupName) {
      this.registrationError = "Full name is required";
    } else if (!this.signupPassword) {
      this.registrationError = "Password is required";
    } else if (this.passwordsDoNotMatch()) {
      this.registrationError = "Passwords do not match";
    }
    return;
  }
  
  // Clear any previous errors
  this.registrationError = '';

  // Create the user data object - SIMPLIFIED without face image
  const userData = {
    name: this.signupName,
    email: this.signupEmail,
    password: this.signupPassword,
    role: this.signupRole
  };
  
  // Log what we're sending
  console.log('Sending simplified registration data:', {
    name: userData.name,
    email: userData.email,
    password: '********',
    role: userData.role
  });
  
  // Show loading indicator
  this.registrationInProgress = true;
  
  // Call the register method
  this.authService.register(userData).subscribe({
    next: (response) => {
      console.log('Registration successful:', response);
      this.registrationInProgress = false;
      this.nextStep(); // Go to success step
      
      // Store user data in localStorage
      localStorage.setItem('userVerified', 'true');
      localStorage.setItem('userRole', this.signupRole);
      localStorage.setItem('userName', this.signupName);
      localStorage.setItem('userData', JSON.stringify(response.user));
    },
    error: (error) => {
      this.registrationInProgress = false;
      this.registrationError = error.message || "Registration failed";
      console.error('Registration error details:', error);
    }
  });
}
loginVerified(): void {
  // Here, handle the successful verification
  console.log('User verified with role:', this.signupRole);
  
  // Store user data in localStorage (this will only be available on port 4200)
  localStorage.setItem('userVerified', 'true');
  localStorage.setItem('userRole', this.signupRole);
  localStorage.setItem('userName', this.signupName);
  localStorage.setItem('userData', JSON.stringify({ 
    name: this.signupName, 
    email: this.signupEmail,
    role: this.signupRole 
  }));

  // Close the modal
  if (this.loginModal) {
    this.loginModal.hide();
  }
  
  // Create URL with query parameters to pass user data
  const userData = encodeURIComponent(JSON.stringify({
    name: this.signupName,
    email: this.signupEmail,
    role: this.signupRole
  }));
  
  // Redirect with user data as query parameters
  switch(this.signupRole) {
    case 'Player':
      window.location.href = `http://localhost:5000/dashboard.html?userData=${userData}`;
      break;
    case 'Agent':
      window.location.href = `http://localhost:5000/agent-dashboard.html?userData=${userData}`;
      break;
    case 'Club Staff':
      window.location.href = `http://localhost:5000/club-dashboard.html?userData=${userData}`;
      break;
    case 'Service Provider':
      window.location.href = `http://localhost:5000/service-provider-dashboard.html?userData=${userData}`;
      break;
    default:
      window.location.href = '/';
  }
}
  
  resetSignupForm() {
    this.signupStep = 1;
    this.signupRole = 'Player';
    this.idDocument = null;
    this.roleDocument = null;
    this.idVerified = false;
    this.roleDocVerified = false;
    this.selfieBase64 = null;
    this.faceVerified = false;
      this.idVerified = false;
  this.roleDocVerified = false;
  this.selfieBase64 = null;
  this.faceVerified = false;
  this.signupName = '';
  this.signupPassword = '';
  this.confirmPassword = '';
  
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