import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PaletteExportDialog } from './palette-export-dialog';

describe('PaletteExportDialog', () => {
  let component: PaletteExportDialog;
  let fixture: ComponentFixture<PaletteExportDialog>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PaletteExportDialog],
    }).compileComponents();

    fixture = TestBed.createComponent(PaletteExportDialog);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
