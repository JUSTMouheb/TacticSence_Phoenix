import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { BehaviorSubject, from, Observable, of, throwError } from 'rxjs';
import { catchError, delay, tap } from 'rxjs/operators';
import { account } from '../config/appwrite.config';
import { OAuthProvider } from 'appwrite';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private apiUrl = 'http://localhost:5000/api';
  private tokenKey = 'auth_token';
  private userKey = 'current_user';
  
  private currentUserSubject = new BehaviorSubject<any>(null);
  public currentUser$ = this.currentUserSubject.asObservable();
  
  constructor(private http: HttpClient) {
    this.loadStoredUser();
  }
  
private loadStoredUser(): void {
    try {
      // Check if we have a stored token and user
      const token = localStorage.getItem(this.tokenKey);
      const userJson = localStorage.getItem(this.userKey);
      
      if (token && userJson) {
        const user = JSON.parse(userJson);
        this.currentUserSubject.next(user);
      }
    } catch (err) {
      console.error('Failed to load stored user', err);
      this.logout(); // Clear potentially corrupt data
    }
  }
 // Get current user synchronously
  getCurrentUserSync(): any {
    return this.currentUserSubject.value;
  }
  signInWithGoogle(): void {
    account.createOAuth2Session(
      'google' as OAuthProvider, 
      `${window.location.origin}`,             // Success URL
      `${window.location.origin}/auth-failed`  // Failure URL
    );
  }
  
  signInWithFacebook(): void {
    account.createOAuth2Session(
      'facebook' as OAuthProvider, 
      `${window.location.origin}`, 
      `${window.location.origin}/auth-failed`
    );
  }
  
signInWithLinkedIn(): void {
  account.createOAuth2Session(
    'linkedin' as OAuthProvider, // Add a space between 'linkedin' and 'as'
    `${window.location.origin}`,
    `${window.location.origin}/auth-failed`
  );
}
  
  // Get current user
  getCurrentUser(): Observable<any> {
    return from(account.get()).pipe(
      tap(user => this.currentUserSubject.next(user))
    );
  }



  processAuthToken(provider: string, code: string): Observable<any> {
  // For Appwrite, we don't need to process the token manually
  // as Appwrite handles the OAuth flow automatically
  // Just return user information after successful login
  return from(account.get()).pipe(
    tap(user => {
      this.currentUserSubject.next(user);
    }),
    catchError(error => {
      console.error('Error processing auth token:', error);
      return of(null);
    })
  );
}
  
  
// Update the register method
register(userData: any): Observable<any> {
  console.log('Sending registration request to:', `${this.apiUrl}/auth/register`);
  
  return this.http.post<any>(`${this.apiUrl}/auth/register`, userData)
    .pipe(
      tap(response => {
        console.log('Registration successful, response:', response);
        if (response && response.token) {
          localStorage.setItem(this.tokenKey, response.token);
          localStorage.setItem(this.userKey, JSON.stringify(response.user));
          this.currentUserSubject.next(response.user);
        }
      }),
      catchError(error => {
        console.error('Registration failed, error details:', error);
        
        // Extract a meaningful error message
        let errorMessage = 'Registration failed';
        
        if (error.error) {
          if (typeof error.error === 'string') {
            errorMessage = error.error;
          } else if (error.error.msg) {
            errorMessage = error.error.msg;
          }
        }
        
        return throwError(() => new Error(errorMessage));
      })
    );
}
  
registerUser(userData: any): Observable<any> {
  // Use either your API or Appwrite
  return from(account.create(
    userData.email,
    userData.password,
    userData.name
  )).pipe(
    tap(response => {
      console.log('User registered:', response);
    }),
    catchError(error => {
      console.error('Registration error:', error);
      throw error;
    })
  );
}

// Add this method to your auth service

loginVerified(userData: any): void {
  // This would handle the verified login
  // For now, we'll just simulate a login
  const mockUser = {
    id: 'verified-user-' + Date.now(),
    name: 'Verified User',
    email: 'verified@example.com',
    role: userData.role,
    verified: true,
    prefs: {
      avatar: null // Will use default avatar
    }
  };
  
  // Set the current user in your service
  this.currentUserSubject.next(mockUser);
  
  // You might also want to store this in localStorage
  localStorage.setItem('currentUser', JSON.stringify(mockUser));
}
  // Check if user is authenticated
  get isAuthenticated(): boolean {
    return this.currentUserSubject.value !== null;
  }
  
 // Login with email and password
  login(email: string, password: string): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}/auth/login`, { email, password })
      .pipe(
        tap(response => {
          // Store token and user in localStorage
          localStorage.setItem(this.tokenKey, response.token);
          localStorage.setItem(this.userKey, JSON.stringify(response.user));
          
          // Update current user subject
          this.currentUserSubject.next(response.user);
        }),
        catchError(error => {
          console.error('Login failed', error);
          return throwError(() => new Error(error.error?.msg || 'Invalid email or password'));
        })
      );
  }
socialLogin(provider: string): Observable<any> {
  // This would typically initiate OAuth flow
  // For demo purposes, return a mock success
  return of({ 
    id: `${provider}-user-123`, 
    name: 'Social User',
    email: `user@${provider}.example`,
    token: 'mock-token'
  }).pipe(
    delay(500),
    tap(user => {
      this.currentUserSubject.next(user);
      localStorage.setItem('currentUser', JSON.stringify(user));
    })
  );
}
// Add this method to your AuthService class
resetPassword(email: string): Observable<any> {
  return this.http.post<any>(`${this.apiUrl}/auth/reset-password`, { email })
    .pipe(
      tap(() => {
        console.log(`Password reset email sent to ${email}`);
      }),
      catchError(error => {
        console.error('Error in reset password:', error);
        return throwError(() => error);
      })
    );
}

 // Logout user
  logout(): void {
    // Remove token and user from localStorage
    localStorage.removeItem(this.tokenKey);
    localStorage.removeItem(this.userKey);
    
    // Clear current user subject
    this.currentUserSubject.next(null);
  }
  
  // Check if user is logged in
  isLoggedIn(): boolean {
    return !!this.getToken();
  }
  
  // Get stored token
  getToken(): string | null {
    return localStorage.getItem(this.tokenKey);
  }
  
  // Get HTTP headers with auth token
  getAuthHeaders(): HttpHeaders {
    const token = this.getToken();
    return new HttpHeaders({
      'Content-Type': 'application/json',
      'x-auth-token': token || ''
    });
  }
requestPasswordReset(email: string): Observable<any> {
  // Implementation depends on your backend API
  return this.http.post<any>(`${this.apiUrl}/auth/reset-password`, { email });
}
}