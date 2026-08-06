import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PaletteSaveDialog } from './palette-save-dialog';

describe('PaletteSaveDialog', () => {
  let component: PaletteSaveDialog;
  let fixture: ComponentFixture<PaletteSaveDialog>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PaletteSaveDialog],
    }).compileComponents();

    fixture = TestBed.createComponent(PaletteSaveDialog);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
