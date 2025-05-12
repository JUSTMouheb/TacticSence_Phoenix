// services/kinde-auth.service.ts
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { BehaviorSubject, Observable, from, of } from 'rxjs';
import { catchError, tap, map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class KindeAuthService {
  // Kinde configuration
  private domain = 'https://esprit.kinde.com';
  private clientId = '861d55685be34995aecd5a8c8ba29e39';
  private redirectUri = window.location.origin + '/callback';
  private scope = 'openid profile email offline';
  
  // User state management
  private userSubject = new BehaviorSubject<any>(null);
  public user$ = this.userSubject.asObservable();
  
  // Authentication state
  private isAuthenticatedSubject = new BehaviorSubject<boolean>(false);
  public isAuthenticated$ = this.isAuthenticatedSubject.asObservable();
  
  constructor(
    private http: HttpClient,
    private router: Router
  ) {
    this.checkSession();
  }
  
  // Check if there's an active session
  private checkSession(): void {
    const token = localStorage.getItem('auth_token');
    const user = localStorage.getItem('user');
    
    if (token && user) {
      this.userSubject.next(JSON.parse(user));
      this.isAuthenticatedSubject.next(true);
    }
  }
  
  // Login with Kinde
  public login(): void {
    this.authorize('login');
  }
  
  // Register with Kinde
  public register(): void {
    this.authorize('registration');
  }
  
  // Create the authorization URL and redirect
  private authorize(startPage: 'login' | 'registration'): void {
    const nonce = this.generateRandomString(16);
    const state = this.generateRandomString(16);
    const codeVerifier = this.generateRandomString(43);
    const codeChallenge = this.generateCodeChallenge(codeVerifier);
    
    // Store PKCE and state values for validation
    localStorage.setItem('code_verifier', codeVerifier);
    localStorage.setItem('state', state);
    
    const authUrl = new URL(`${this.domain}/oauth2/auth`);
    authUrl.searchParams.append('client_id', this.clientId);
    authUrl.searchParams.append('redirect_uri', this.redirectUri);
    authUrl.searchParams.append('response_type', 'code');
    authUrl.searchParams.append('scope', this.scope);
    authUrl.searchParams.append('state', state);
    authUrl.searchParams.append('nonce', nonce);
    authUrl.searchParams.append('code_challenge', codeChallenge);
    authUrl.searchParams.append('code_challenge_method', 'S256');
    
    // Add start_page to specify login or registration
    authUrl.searchParams.append('start_page', startPage);
    
    window.location.href = authUrl.toString();
  }
  
  // Handle the authentication callback
  public handleCallback(code: string, state: string): Observable<any> {
    const storedState = localStorage.getItem('state');
    const codeVerifier = localStorage.getItem('code_verifier');
    
    if (!storedState || state !== storedState) {
      return of(null).pipe(
        tap(() => {
          console.error('Invalid state parameter');
          this.router.navigate(['/']);
        })
      );
    }
    
    // Clean up stored state
    localStorage.removeItem('state');
    
    // Create token exchange request
    const tokenRequest = {
      grant_type: 'authorization_code',
      client_id: this.clientId,
      code_verifier: codeVerifier,
      code: code,
      redirect_uri: this.redirectUri
    };
    
    // Exchange the code for tokens
    return this.http.post<any>(
      `${this.domain}/oauth2/token`, 
      new URLSearchParams(tokenRequest as any).toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    ).pipe(
      tap(response => {
        // Store tokens and user info
        localStorage.setItem('auth_token', response.access_token);
        localStorage.setItem('id_token', response.id_token);
        localStorage.setItem('refresh_token', response.refresh_token);
        
        // Get user info using the access token
        this.getUserInfo().subscribe();
      }),
      catchError(error => {
        console.error('Token exchange error:', error);
        return of(null);
      })
    );
  }
  
  // Get the user's profile information
  public getUserInfo(): Observable<any> {
    const token = localStorage.getItem('auth_token');
    
    if (!token) {
      return of(null);
    }
    
    return this.http.get<any>(`${this.domain}/oauth2/v2/user_profile`, {
      headers: { 'Authorization': `Bearer ${token}` }
    }).pipe(
      tap(user => {
        localStorage.setItem('user', JSON.stringify(user));
        this.userSubject.next(user);
        this.isAuthenticatedSubject.next(true);
      }),
      catchError(error => {
        console.error('User info error:', error);
        return of(null);
      })
    );
  }
  
  // Get the current user
  public getUser(): Observable<any> {
    return this.user$;
  }
  
  // Get the access token
  public getToken(): string | null {
    return localStorage.getItem('auth_token');
  }
  
  // Log out the user
  public logout(): void {
    const token = localStorage.getItem('id_token');
    
    // Clear local storage
    localStorage.removeItem('auth_token');
    localStorage.removeItem('id_token');
    localStorage.removeItem('refresh_token');
    localStorage.removeItem('user');
    
    // Update state
    this.userSubject.next(null);
    this.isAuthenticatedSubject.next(false);
    
    // Redirect to Kinde logout endpoint
    const logoutUrl = new URL(`${this.domain}/logout`);
    logoutUrl.searchParams.append('id_token_hint', token || '');
    logoutUrl.searchParams.append('post_logout_redirect_uri', window.location.origin);
    
    window.location.href = logoutUrl.toString();
  }
  
  // PKCE Helper: Generate a random string
  private generateRandomString(length: number): string {
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~';
    let text = '';
    
    for (let i = 0; i < length; i++) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    
    return text;
  }
  
  // PKCE Helper: Generate code challenge
  private generateCodeChallenge(codeVerifier: string): string {
    // This is a simplified version - in a real app you'd use the crypto API
    // For demonstration purposes only
    return btoa(codeVerifier)
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');
  }
}