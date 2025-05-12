import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { BehaviorSubject, Observable, from, of, throwError } from 'rxjs';
import { map, tap, catchError, delay } from 'rxjs/operators';
import { account } from '../config/appwrite.config';
import { OAuthProvider } from 'appwrite';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private currentUserSubject = new BehaviorSubject<any>(null);
  currentUser$ = this.currentUserSubject.asObservable();
  private apiUrl = 'http://localhost:8000/api'; // Your API URL
  
  constructor(private http: HttpClient) {
    // Check for existing session on load
    this.checkSession();
  }
  
  private checkSession() {
    from(account.get()).pipe(
      tap(user => {
        this.currentUserSubject.next(user);
      })
    ).subscribe({
      error: () => {
        // User is not logged in, do nothing
        console.log('No active session found');
      }
    });
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
  
  // Logout
  logout(): Observable<any> {
    return from(account.deleteSession('current')).pipe(
      tap(() => this.currentUserSubject.next(null))
    );
  }
 // Add this method to your AuthService if not already present

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
  
login(email: string, password: string): Observable<any> {
  // Implementation depends on your backend API
  return this.http.post<any>(`${this.apiUrl}/auth/login`, { email, password }).pipe(
    tap(user => {
      this.currentUserSubject.next(user);
      localStorage.setItem('currentUser', JSON.stringify(user));
    }),
    catchError(error => {
      console.error('Login error', error);
      return throwError(() => error);
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
requestPasswordReset(email: string): Observable<any> {
  // Implementation depends on your backend API
  return this.http.post<any>(`${this.apiUrl}/auth/reset-password`, { email });
}
}